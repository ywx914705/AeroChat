/*
AeroChat之所以高性能,离不开AeroQueue!!!
AeroQueue是AeroChat项目中一个轻量级的异步任务队列,
它的主要作用是将可能阻塞的耗时操作(如数据库查询、Redis操作、文件 I/O 等)
可以和MySQL配合使用
从Reactor 线程中分离出来,放到独立的后台线程池中执行,从而避免阻塞事件循环,
保证服务器的高响应性。
*/
#pragma once
#include <atomic>
#include <functional>
#include <thread>
#include <vector>
#include "concurrentqueue.hpp"

class AeroQueue {
public:
    using Task = std::function<void()>;

    static AeroQueue& instance();//单例获取,AeroChat中有且仅有一个AeroQueue实例
    //通过 instance() 获取全局唯一的AeroQueue实例

    void start(size_t threadCount = 16);//启动线程池
    void stop(bool wait = true);//停止线程池
    void post(Task task);//提交任务
    ~AeroQueue();

private:
    AeroQueue() = default;
    void workerThread();

    std::vector<std::thread> threads_;//线程池
    moodycamel::ConcurrentQueue<Task> tasks_;//无锁队列(各种任务的集合)
    //来存储待处理的任务，实现多生产者多消费者模式，无需加锁
    
    std::atomic<bool> stopped_{false};//停止标志
};
