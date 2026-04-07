#include "chatserver.hpp"
#include "Log.hpp"
#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

std::atomic<int> ChatServer::connCount_(0);

ChatServer::ChatServer(EventLoop* loop, uint16_t port, int numSubReactors)
    : loop_(loop), port_(port),
      threadPool_(new EventLoopThreadPool(loop, numSubReactors)),
      listenFd_(-1), acceptChannel_(nullptr) {

    struct rlimit rlim;
    if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
        rlim.rlim_cur = MAX_CONNECTIONS + 1000;
        rlim.rlim_max = MAX_CONNECTIONS + 1000;
        setrlimit(RLIMIT_NOFILE, &rlim);
    }
}

ChatServer::~ChatServer() {
    if (listenFd_ >= 0) close(listenFd_);
}

void ChatServer::start() {
    threadPool_->start();

    listenFd_ = sock_.Socket();
    int reuse = 1;
    setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    setsockopt(listenFd_, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse));
    fcntl(listenFd_, F_SETFL, O_NONBLOCK | FD_CLOEXEC);
    sock_.Bind(listenFd_, port_);
    sock_.Listen(listenFd_, 8192);

    acceptChannel_ = std::make_unique<Channel>(loop_, listenFd_);
    acceptChannel_->setReadCallback([this] { handleAccept(); });
    acceptChannel_->enableReading();
    acceptChannel_->enableET();
    loop_->updateChannel(acceptChannel_.get());

    LOG_INFO("[EchoServer] Started on port " + std::to_string(port_));
}

void ChatServer::handleAccept() {
    while (true) {
        std::string clientIp;
        uint16_t clientPort;
        int connFd = sock_.Accept(listenFd_, &clientIp, &clientPort);
        if (connFd < 0) {
            if (errno == EAGAIN) break;
            LOG_ERROR("Accept error");
            break;
        }
        if (connCount_ >= MAX_CONNECTIONS) {
            close(connFd);
            continue;
        }
        EventLoop* subLoop = threadPool_->getNextLoop();
        subLoop->runInLoop([this, connFd, subLoop] {
            handleNewConn(connFd, subLoop);
        });
    }
}

void ChatServer::handleNewConn(int connFd, EventLoop* subLoop) {
    // 设置 TCP_NODELAY
    int nodelay = 1;
    setsockopt(connFd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    int flags = fcntl(connFd, F_GETFL, 0);
    fcntl(connFd, F_SETFL, flags | O_NONBLOCK | FD_CLOEXEC);

    auto user = std::make_shared<User>(connFd, subLoop);

    // Echo 回调
    user->setMessageCallback([](int fd, const std::string& msg, std::shared_ptr<User> u) {
        u->send(msg.data(), msg.size());
    });

    user->setErrorCallback([this, connFd](int fd) {
        connCount_--;
    });

    user->start();
    connCount_++;
}
