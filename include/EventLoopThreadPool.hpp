/*
EventLoopThreadPool是AeroChat中管理多个子Reactor线程的核心类,它封装了多个EventLoopThead,对外提供
统一的接口来获取子Reactor的EventLoop,负责新连接的负载均衡
*/#ifndef EVENTLOOPTHREADPOOL_HPP
#define EVENTLOOPTHREADPOOL_HPP

#include <vector>
#include <memory>
#include <string>
#include "EventLoop.hpp"
#include "EventLoopThread.hpp"
#include<atomic>

class EventLoopThreadPool {
public:
    EventLoopThreadPool(EventLoop* baseLoop, int numThreads);
    ~EventLoopThreadPool() = default;

    void start();//开启所以线程,进行loop
    EventLoop* getNextLoop();          // 无参获取下一个loop
    EventLoop* getLoop(int idx);       // 根据索引获取指定loop
    int getThreadNum() const;          // 获取线程数（仅声明）
    std::vector<EventLoop*> getAllLoops() const; // 获取所有loop（仅声明）

private:
    EventLoop* baseLoop_;              // 主Reactor的loop
    int numThreads_;                   // 子Reactor数量
    std::atomic<int>next_{0};                         // 轮询索引
    bool started_;                     // 是否启动
    std::vector<std::unique_ptr<EventLoopThread>> threads_; // 子线程列表
    std::vector<EventLoop*> loops_;    // 子loop列表 存储所以EventLoop对应的指针
};

#endif // EVENTLOOPTHREADPOOL_HPP