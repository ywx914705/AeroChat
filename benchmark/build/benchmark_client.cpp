#include <iostream>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/epoll.h>
#include <signal.h>

// 全局统计
std::atomic<long> total_sent{0};
std::atomic<long> total_recv{0};
std::atomic<long> total_time_us{0};
std::atomic<long> min_latency_us{1000000000};
std::atomic<long> max_latency_us{0};
std::vector<long> latencies;
std::mutex lat_mutex;

// 配置参数
struct Config {
    std::string server_ip = "127.0.0.1";
    int server_port = 2048;
    int connections_per_thread = 1000;
    int num_threads = 4;
    int msg_size = 64;
    int send_qps_per_conn = 10;
    int duration_sec = 60;
};

struct Connection {
    int fd;
    struct sockaddr_in addr;
    std::string send_buf;
    std::string recv_buf;
    std::chrono::steady_clock::time_point send_time;
    bool waiting_response = false;
};

class PerfClient {
public:
    PerfClient(const Config& cfg, int thread_id)
        : cfg_(cfg), thread_id_(thread_id), stop_(false), epoll_fd_(-1) {}

    ~PerfClient() { if (epoll_fd_ >= 0) close(epoll_fd_); }

    void run() {
        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) {
            perror("epoll_create1");
            return;
        }

        // 建立连接
        for (int i = 0; i < cfg_.connections_per_thread; ++i) {
            int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
            if (fd < 0) {
                perror("socket");
                continue;
            }

            struct sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_port = htons(cfg_.server_port);
            inet_pton(AF_INET, cfg_.server_ip.c_str(), &addr.sin_addr);

            int ret = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
            if (ret < 0 && errno != EINPROGRESS) {
                close(fd);
                continue;
            }

            struct epoll_event ev;
            ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
            ev.data.fd = fd;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
                close(fd);
                continue;
            }

            Connection conn;
            conn.fd = fd;
            conn.addr = addr;
            conn.waiting_response = false;
            std::string msg = std::string(cfg_.msg_size, 'A') + "\n";
            conn.send_buf = msg;
            conn.recv_buf.reserve(65536);
            connections_[fd] = conn;
        }

        std::cout << "[Thread " << thread_id_ << "] Created " << connections_.size() << " connections\n";

        auto last_tick = std::chrono::steady_clock::now();
        long msgs_to_send = 0;
        if (cfg_.send_qps_per_conn > 0) {
            msgs_to_send = cfg_.send_qps_per_conn * connections_.size() / 1000;
        }

        const int MAX_EVENTS = 10000;
        struct epoll_event events[MAX_EVENTS];
        auto start_time = std::chrono::steady_clock::now();

        while (!stop_) {
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count() >= cfg_.duration_sec) {
                break;
            }

            // 发送速率控制
            if (cfg_.send_qps_per_conn > 0) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_tick).count();
                if (elapsed >= 1) {
                    long to_send = msgs_to_send * elapsed;
                    if (to_send > 0) {
                        int sent = 0;
                        for (auto& pair : connections_) {
                            if (sent >= to_send) break;
                            Connection& conn = pair.second;
                            if (!conn.waiting_response) {
                                send_message(conn);
                                sent++;
                            }
                        }
                    }
                    last_tick = now;
                }
            } else {
                for (auto& pair : connections_) {
                    Connection& conn = pair.second;
                    if (!conn.waiting_response) {
                        send_message(conn);
                    }
                }
            }

            int nfds = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1);
            for (int i = 0; i < nfds; ++i) {
                int fd = events[i].data.fd;
                auto it = connections_.find(fd);
                if (it == connections_.end()) continue;
                Connection& conn = it->second;

                if (events[i].events & EPOLLOUT) {
                    int err = 0;
                    socklen_t len = sizeof(err);
                    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0) {
                        if (!conn.waiting_response) {
                            send_message(conn);
                        }
                    } else {
                        close(fd);
                        connections_.erase(it);
                        continue;
                    }
                    // 移除 EPOLLOUT 监听
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = fd;
                    epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
                }
                if (events[i].events & EPOLLIN) {
                    handle_read(conn);
                }
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    close(fd);
                    connections_.erase(it);
                }
            }
        }

        for (auto& pair : connections_) {
            close(pair.first);
        }
    }

    void stop() { stop_ = true; }

private:
    void send_message(Connection& conn) {
        ssize_t n = write(conn.fd, conn.send_buf.data(), conn.send_buf.size());
        if (n == (ssize_t)conn.send_buf.size()) {
            conn.waiting_response = true;
            conn.send_time = std::chrono::steady_clock::now();
            total_sent++;
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct epoll_event ev;
            ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
            ev.data.fd = conn.fd;
            epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn.fd, &ev);
        } else {
            close(conn.fd);
            connections_.erase(conn.fd);
        }
    }

    void handle_read(Connection& conn) {
        char buf[65536];
        while (true) {
            ssize_t n = read(conn.fd, buf, sizeof(buf));
            if (n > 0) {
                conn.recv_buf.insert(conn.recv_buf.end(), buf, buf + n);
            } else if (n == 0) {
                close(conn.fd);
                connections_.erase(conn.fd);
                return;
            } else {
                if (errno == EAGAIN) break;
                close(conn.fd);
                connections_.erase(conn.fd);
                return;
            }
        }

        size_t pos = 0;
        while (pos < conn.recv_buf.size()) {
            auto it = std::find(conn.recv_buf.begin() + pos, conn.recv_buf.end(), '\n');
            if (it == conn.recv_buf.end()) break;
            size_t len = (it - conn.recv_buf.begin()) - pos + 1;
            if (conn.waiting_response) {
                auto now = std::chrono::steady_clock::now();
                long us = std::chrono::duration_cast<std::chrono::microseconds>(now - conn.send_time).count();
                total_recv++;
                total_time_us += us;
                {
                    std::lock_guard<std::mutex> lock(lat_mutex);
                    if (us < min_latency_us) min_latency_us = us;
                    if (us > max_latency_us) max_latency_us = us;
                    if (latencies.size() < 100000) {
                        latencies.push_back(us);
                    }
                }
                conn.waiting_response = false;
            }
            pos += len;
        }
        if (pos > 0) {
            conn.recv_buf.erase(conn.recv_buf.begin(), conn.recv_buf.begin() + pos);
        }
    }

    Config cfg_;
    int thread_id_;
    std::atomic<bool> stop_;
    int epoll_fd_;
    std::unordered_map<int, Connection> connections_;
};

int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);

    Config cfg;
    if (argc > 1) cfg.server_ip = argv[1];
    if (argc > 2) cfg.server_port = std::stoi(argv[2]);
    if (argc > 3) cfg.connections_per_thread = std::stoi(argv[3]);
    if (argc > 4) cfg.num_threads = std::stoi(argv[4]);
    if (argc > 5) cfg.msg_size = std::stoi(argv[5]);
    if (argc > 6) cfg.send_qps_per_conn = std::stoi(argv[6]);
    if (argc > 7) cfg.duration_sec = std::stoi(argv[7]);

    std::cout << "=== AeroChat Benchmark Client ===\n";
    std::cout << "Server: " << cfg.server_ip << ":" << cfg.server_port << "\n";
    std::cout << "Threads: " << cfg.num_threads << ", Connections per thread: " << cfg.connections_per_thread << "\n";
    std::cout << "Msg size: " << cfg.msg_size << " bytes, QPS per conn: " << cfg.send_qps_per_conn << "\n";
    std::cout << "Duration: " << cfg.duration_sec << " seconds\n";

    std::vector<std::thread> threads;
    std::vector<std::unique_ptr<PerfClient>> clients;
    for (int i = 0; i < cfg.num_threads; ++i) {
        clients.push_back(std::make_unique<PerfClient>(cfg, i));
    }
    auto start = std::chrono::steady_clock::now();
    for (auto& client : clients) {
        threads.emplace_back([&client] { client->run(); });
    }
    for (auto& t : threads) t.join();
    auto end = std::chrono::steady_clock::now();
    double duration = std::chrono::duration<double>(end - start).count();

    long sent = total_sent.load();
    long recv = total_recv.load();
    long total_us = total_time_us.load();
    double avg_us = (recv > 0) ? (double)total_us / recv : 0;
    long min_us = min_latency_us.load();
    long max_us = max_latency_us.load();

    std::sort(latencies.begin(), latencies.end());
    long p95 = 0, p99 = 0;
    if (!latencies.empty()) {
        p95 = latencies[(size_t)(latencies.size() * 0.95)];
        p99 = latencies[(size_t)(latencies.size() * 0.99)];
    }

    std::cout << "\n=== Results ===\n";
    std::cout << "Total sent: " << sent << "\n";
    std::cout << "Total received: " << recv << "\n";
    std::cout << "Duration: " << duration << " s\n";
    std::cout << "Throughput (QPS): " << (recv / duration) << " msg/s\n";
    std::cout << "Average latency: " << avg_us / 1000.0 << " ms\n";
    std::cout << "Min latency: " << min_us / 1000.0 << " ms\n";
    std::cout << "Max latency: " << max_us / 1000.0 << " ms\n";
    std::cout << "P95 latency: " << p95 / 1000.0 << " ms\n";
    std::cout << "P99 latency: " << p99 / 1000.0 << " ms\n";

    return 0;
}
