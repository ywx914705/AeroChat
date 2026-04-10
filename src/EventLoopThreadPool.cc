#include "EventLoopThreadPool.hpp"
#include "Log.hpp"
#include <atomic>   // 新增，用于原子操作

EventLoopThreadPool::EventLoopThreadPool(EventLoop* baseLoop, int numThreads)
    : baseLoop_(baseLoop), numThreads_(numThreads), started_(false) {
    // 注意：next_ 已在头文件中声明为 std::atomic<int> 并初始化为 0，因此构造函数中不再需要初始化它
    if (numThreads_ < 0) {
        LOG_ERROR("numThreads must be >=0");
        numThreads_ = 0;
    }
}

void EventLoopThreadPool::start() {
   //启动所有Reactor对应的线程,开启事件循环(loop)
    if (started_) return;
    started_ = true;
    for (int i = 0; i < numThreads_; ++i) {
        auto t = std::make_unique<EventLoopThread>("SubReactor-" + std::to_string(i));
        EventLoop* subLoop = t->startLoop();
        loops_.push_back(subLoop);
        threads_.push_back(std::move(t));
    }
}

EventLoop* EventLoopThreadPool::getNextLoop() {
    if (loops_.empty()) return baseLoop_;
    size_t size = loops_.size();
    // 原子递增，返回递增前的值，取模得到索引
    int idx = next_.fetch_add(1, std::memory_order_relaxed) % static_cast<int>(size);
    return loops_[idx];
}

EventLoop* EventLoopThreadPool::getLoop(int idx) {
    //增加一个有消息检查
    if(loops_.empty())return baseLoop_;
    if (idx < 0 || static_cast<size_t>(idx) >= loops_.size()) {
        return baseLoop_;
    }
    return loops_[idx];
}

int EventLoopThreadPool::getThreadNum() const {
    return numThreads_;
}

std::vector<EventLoop*> EventLoopThreadPool::getAllLoops() const {
    return loops_;
}