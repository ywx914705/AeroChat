#pragma once
#include <functional>
#include <sys/epoll.h>

class EventLoop;

enum ChannelIndexStatus {
    kNew = -1,//未添加到epoll中
    kAdded = 0,//已添加到epoll中
    kDeleted = 1//已从epoll中删除
};
/*
Channel:
Channel是Reactor模式中的核心组件,它封装了一个文件描述符(fd)以及在epoll中关注的事件和对应的事件回调函数,它不拥有fd,只负责
事件的注册、更新、以及分发,每个Channel对象都绑定到一个EventLoop,一个EventLoop对应一个Poller对应一个线程,该EventLoop/该线程
负责实际的epoll操作(epoll_create,epoll_ctl,epoll_wait)
如何在这个AeroChat中进行作用？
每个客户端连接(User)拥有一个Channel,该Channel的fd就是客户端的socket,User在构造时创建Channel,并设置读写回调,
当epoll中检测到事件时,Poller会调用对应Channel的handleEvent!
*/
class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd)
        : loop_(loop), fd_(fd), events_(0), revents_(0), index_(-1) {}

    ~Channel() = default;

    void handleEvent();

    //设置回调方法
    void setReadCallback(EventCallback cb) { readCallback_ = std::move(cb); }
    void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
    void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }
    void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }
    //用户通常会在创建 User 或 ChatServer 时设置这些回调，将业务逻辑与 I/O 事件绑定。
    
    void enableReading() {//设置为可读事件,表示我们监听该fd上的读事件
        events_ |= EPOLLIN | EPOLLPRI;
        update();
    }
    void disableReading() {
        events_ &= ~(EPOLLIN | EPOLLPRI);
        update();
    }
    void enableWriting() {
        events_ |= EPOLLOUT;
        update();
    }
    void disableWriting() {
        events_ &= ~EPOLLOUT;
        update();
    }
    void disableAll() {
        events_ = 0;
        update();
    }

    void enableET();//
    void disableET();
    //用于快速判断当前是否关注读写事件。
    bool isWriting() const { return events_ & EPOLLOUT; }
    bool isReading() const { return events_ & (EPOLLIN | EPOLLPRI); }

    int fd() const { return fd_; }
    uint32_t events() const { return events_; }
    void setRevents(uint32_t revt) { revents_ = revt; }
    int index() const { return index_; }
    void setIndex(int idx) { index_ = idx; }
    EventLoop* ownerLoop() const { return loop_; }
    void update();//委托给所属 EventLoop的updateChannel方法,最终调用 Poller::updateChannel执行实际的epoll_ctl。

private:
    EventLoop* loop_;//所属的事件循环
    const int fd_;//文件描述符
    uint32_t events_;//当前关注的事件(如EPOLLIN/EPOLLOUT/EPOLLET)
    uint32_t revents_;//epoll_wait所返回实际发生的事件
    int index_;//在Poller中的状态(kNew/kAdded/kDeleted)
     //读、写、关闭、错误对应的事件回调  回调函数:用户注册的业务处理函数,当对应事件发生时被调用
    EventCallback readCallback_;
    EventCallback writeCallback_;
    EventCallback closeCallback_;
    EventCallback errorCallback_;
};