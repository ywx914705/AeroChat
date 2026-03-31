/*
User类封装了一个客户端连接的所有状态和操作,是 AeroChat 中处理单个用户的核心。它集成了非阻塞 I/O、事件驱动(通过 Channel)、
读写缓冲区、登录状态、活动时间以及回调机制。User代表一个客户端连接(即代表一个用户)
User拥有一个Channel,在构造函数中创建Channel,并将自己的fd和所属的EventLoop传递给它
设置Channel的回调,在start()中将handlerRead以及handlerWrite绑定到Channel的读/写回调
*/
#ifndef USER_HPP
#define USER_HPP

#include "EventLoop.hpp"
#include "Channel.hpp"
#include <memory>
#include <functional>
#include <atomic>
#include <string>
#include <vector>

//User继承于enable_shared_from_this<User>,使得在成员函数中通过shared_from_this()安全地获取指向当前对象的shared_ptr
class User : public std::enable_shared_from_this<User> {
public:
    using MessageCallback = std::function<void(int fd, const std::string& msg)>;
    //消息回调(收到完整消息,以\n结尾时,参数为fd和消息内容)
    using ErrorCallback = std::function<void(int fd)>;
    //当连接发生错误或者关闭时调用
    User(int fd, EventLoop* loop);//接受fd和所属的EventLoop(即该用户的连接在哪个Reactor线程中处理)
    ~User() = default;

    // 禁止拷贝
    User(const User&) = delete;//用户对象不可复制！！！
    User& operator=(const User&) = delete;

    // 启动,注册读事件
    void start();

    // 发送数据
    void send(const char* data, size_t len);

    // 获取fd和所属 loop
    int fd() const { return fd_; }
    EventLoop* getLoop() const { return loop_; }

    // 设置回调
    void setMessageCallback(MessageCallback cb);
    void setErrorCallback(ErrorCallback cb);

    // 登录状态
    void setLoggedIn(bool status);
    bool isLoggedIn() const;

    // 用户 ID (仅用于快速识别，不作为唯一源)
    void setUserId(int id);
    int getUserId() const;

    // 活动时间
    void updateActiveTime();
    time_t lastActiveTime() const { return lastActive_; }
    bool isAlive() const { return isAlive_; }

    // 待发送字节数（用于背压）
    size_t pendingBytes() const { return writeBuf_.size() - writePos_; }

    // 处理错误（通常由 Channel 调用或外部触发）
    void handleError();

private:
    void handleRead();
    void handleWrite();
    void processReadBuffer();

    const int fd_;
    EventLoop* const loop_;
    std::unique_ptr<Channel> channel_;

    // 读缓冲区
    std::vector<char> readBuf_;

    // 写缓冲区
    std::vector<char> writeBuf_;
    size_t writePos_;   // 已发送位置

    int userId_;                      // 用户 ID（来自 account）
    std::atomic<bool> isLoggedIn_;    // 是否已登录
    std::atomic<bool> isAlive_;       // 连接是否有效
    std::atomic<bool> closing_;       // 是否正在关闭

    time_t lastActive_;               // 最后活动时间

    MessageCallback messageCb_;
    ErrorCallback errorCb_;

    static const size_t MAX_WRITE_PER_LOOP = 65536; // 每次最多写 64KB
   static const size_t MAX_READ_BUFFER    = 256 * 1024; // 读缓冲区总大小上限 4MB
};

#endif // USER_HPP