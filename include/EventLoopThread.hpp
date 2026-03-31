/*
one loop per thread的具体实现
EventLoopThread的目的就是将EventLoop对象与一个独立线程绑定,使该线程运行事件循环,实现一个线程
一个事件循环
这个类主要负责:
1、创建线程,在线程中构造EventLoop对象(栈上)
2、启动时间循环,直到线程退出
3、提供同步机制，确保调用者能获得已经就绪的EventLoop指针
EventLoopThread是如何与Thread建立联系的？

thread_.start() 内部调用 pthread_create(&tid_, nullptr, runInThread, this)。

新线程启动后,执行 runInThread(this)。

runInThread将this 转回 Thread*并调用 thread->func_(),
也就是执行 EventLoopThread::threadFunc。
于是,EventLoopThread::threadFunc就在子线程中运行,创建并运行EventLoop。

*/
#ifndef EVENTLOOPTHREAD_HPP
#define EVENTLOOPTHREAD_HPP
#include "Thread.hpp"
#include "EventLoop.hpp"
#include <condition_variable>
#include <mutex>
#include <string>

class EventLoopThread : noncopyable {
public:
    explicit EventLoopThread(const std::string& name = std::string());
    ~EventLoopThread();

    EventLoop* startLoop();//启动线程,返回关联的EventLoop指针

private:
    void threadFunc();//线程函数

    EventLoop* loop_;//指向本线程中所创建的EventLoop对象
    bool exiting_;//是否退出
    Thread thread_;//对应的线程
    std::mutex mutex_;//互斥锁
    std::condition_variable cond_;//条件变量
};

#endif // EVENTLOOPTHREAD_HPP
