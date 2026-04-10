#ifndef CHATSERVER_HPP
#define CHATSERVER_HPP

#include "EventLoop.hpp"
#include "EventLoopThreadPool.hpp"
#include "User.hpp"
#include "Sock.hpp"
#include "Channel.hpp"
#include "SessionManager.hpp"
#include <memory>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <string>
#include <functional>
#include <shared_mutex>
#include <sys/timerfd.h>

/*
AeroChat的主控类,主要负责:
1、初始化网络环境以及数据库(Mysql/redis)以及线程池
2、创建监听soket,并接收客户端连接
3、将新连接分发给SubReactor
4、管理所有连接(每个线程都有独立的User映射,减少锁竞争)
5、定时检查空闲连接,自动断开
*/
class ChatServer {
public:
    ChatServer(EventLoop* loop, uint16_t port, int numSubReactors);
    ~ChatServer();

    void start();
    void handleAccept();
    void handleNewConn(int connFd, EventLoop* subLoop,
                       const std::string& clientIp, uint16_t clientPort);
    void removeUser(int fd);
    void checkIdleConnections();

    static std::atomic<int> connCount_;//用来记录当前的连接数

private:
    EventLoop* loop_;//主Reactor的EventLoop(对应主线程)
    uint16_t port_;
    std::unique_ptr<EventLoopThreadPool> threadPool_;//线程池:管理SubReactor
    int listenFd_;//监听socket的fd
    std::unique_ptr<Channel> acceptChannel_;//监听socket的fd对应的Channael
    Sock sock_;//Sock封装 用于创建、绑定、监听也就是linux中tcp服务器常用函数socket()、bind()、listen()
    int epollTimeout_;//epoll_wait超时时间

    // 每个子Reactor对应的用户映射 + 读写锁
    struct PerLoopUsers {
        std::shared_mutex mutex;//读写锁:保护本线程的用户映射
        std::unordered_map<int, std::shared_ptr<User>> map;//fd->User 一个fd对应一个User也就是一个用户
    };
    // 使用 unique_ptr 使 PerLoopUsers 可移动（因为 mutex 不可移动）
    std::vector<std::unique_ptr<PerLoopUsers>> perLoopUsers_;//所以Reactor(线程的)PerLoopUsers

    // 空闲超时相关
    int timerFd_;
    std::unique_ptr<Channel> timerChannel_;
    static const int kIdleTimeoutSeconds = 180;   // 空闲超时 180 秒（原 60 秒，避免压测误踢）

    // Redis 订阅线程
    std::unique_ptr<std::thread> redisSubThread_;
    std::atomic<bool> redisSubRunning_{false};
    void redisSubLoop();
    void broadcastOnlineUpdate(const std::string& msg);
};

#endif // CHATSERVER_HPP