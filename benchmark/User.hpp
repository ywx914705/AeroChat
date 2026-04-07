#ifndef USER_HPP
#define USER_HPP

#include "EventLoop.hpp"
#include "Channel.hpp"
#include <memory>
#include <functional>
#include <atomic>
#include <string>
#include <vector>

class User : public std::enable_shared_from_this<User> {
public:
    using MessageCallback = std::function<void(int fd, const std::string& msg, std::shared_ptr<User>)>;
    using ErrorCallback = std::function<void(int fd)>;

    User(int fd, EventLoop* loop);
    ~User() = default;

    void start();
    void send(const char* data, size_t len);
    int fd() const { return fd_; }
    EventLoop* getLoop() const { return loop_; }
    void setMessageCallback(MessageCallback cb) { messageCb_ = std::move(cb); }
    void setErrorCallback(ErrorCallback cb) { errorCb_ = std::move(cb); }

private:
    void handleRead();
    void handleWrite();
    void handleError();

    const int fd_;
    EventLoop* const loop_;
    std::unique_ptr<Channel> channel_;
    std::vector<char> readBuf_;
    std::vector<char> writeBuf_;
    size_t writePos_ = 0;
    std::atomic<bool> isAlive_{true};
    std::atomic<bool> closing_{false};

    MessageCallback messageCb_;
    ErrorCallback errorCb_;
};

#endif