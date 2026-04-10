#include "Channel.hpp"
#include "EventLoop.hpp"
#include "Log.hpp"
#include <iostream>
void Channel::handleEvent() {  //该函数由EventLoop::loop()收到epoll事件后进行调用,且执行在所属的
//EventLoop线程中,保证了线程安全
    if (revents_ & (EPOLLRDHUP | EPOLLHUP)) { //如果对端关闭连接或者挂起,此时连接不可用,直接关闭
        LOG_WARN("[Channel] FD " + std::to_string(fd_) + " 客户端关闭连接");
        if (closeCallback_) closeCallback_();
        return;
    }
    if (revents_ & EPOLLERR) {
        LOG_ERROR("[Channel] FD " + std::to_string(fd_) + " 发生错误");
        if (errorCallback_) errorCallback_();
    }
    if (revents_ & (EPOLLIN | EPOLLPRI)) {
        if (readCallback_) readCallback_();
    }
    if (revents_ & EPOLLOUT) {
        if (writeCallback_) writeCallback_();
    }
}

//通知EventLoop进行更新        Channel不直接操作epoll,而是通过EventLoop统一管理
void Channel::update() {
    loop_->updateChannel(this);
}

void Channel::enableET() {  //使用ET模式
    //AeroChat采用ET模式配合非阻塞IO 在ChatServer中,监听socket和每个连接的Channel都默认启用ET,以提高性能。
    events_ |= EPOLLET;
    update();
}

void Channel::disableET() {  //取消ET模式
    events_ &= ~EPOLLET;
    update();
}