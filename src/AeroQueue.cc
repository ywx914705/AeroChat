//AeroQueue的实现
#include "AeroQueue.hpp"
#include "Log.hpp"
#include <iostream>
#include <chrono>
#include <iterator>

AeroQueue& AeroQueue::instance() {
    static AeroQueue queue;
    return queue;
}
//启动线程池
void AeroQueue::start(size_t threadCount) {
    stopped_ = false;
	//创建指定数量的线程,然后每个线程执行wokkerThread
    for (size_t i = 0; i < threadCount; ++i) {
        threads_.emplace_back(&AeroQueue::workerThread, this);
		//&AeroQueue::workerThread是一个成员函数指针,指向AeroQueue::workerThread这个成员函数
		//this就是当前AeroQueue对象的指针,这个构造方式等价于std::thread(AeroQueue::worketThread,this)
		//就是创建一个新线程,然后线程入口函数就是AeroQueue::workerThread,并传入this作为隐式参数)
    }
    LOG_INFO("[AeroQueue] 已启动 " + std::to_string(threadCount) + " 个工作线程");
}

void AeroQueue::stop(bool wait) {
    stopped_ = true;
    // 唤醒所有等待的线程，使它们能检查 stopped_ 标志并退出
    cv_.notify_all();
    if (wait) {
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
    }
}

void AeroQueue::post(Task task) {
    tasks_.enqueue(std::move(task));
    // 通知一个工作线程有任务可用，避免所有线程都在等待
    cv_.notify_one();
}

void AeroQueue::workerThread() {
    while (!stopped_) {
        Task task;
        if (tasks_.try_dequeue(task)) {
			//尝试从队列中弹出一个任务,如果成功则返回true,并将弹出任务赋值给task,如果队列为空则返回
			//false(非阻塞)  这种非阻塞特性使线程在队列为空的时候不会一直忙等待,而是可以去做其他事
			try {
                task();//执行任务 任务是一个std::function<void()>,调用task()就会执行用户提交的函数
            } catch (const std::exception& e) {
                LOG_ERROR("[AeroQueue] 任务异常: " + std::string(e.what()));
            } catch (...) {
                LOG_ERROR("[AeroQueue] 未知异常");
            }
        } else {
            // 没有任务时，使用条件变量阻塞，直到有新任务或停止信号
            // 相比原来的 sleep_for(10us)，这种方式能立即响应新任务，避免固定延迟带来的性能下降
            std::unique_lock<std::mutex> lock(cv_mutex_);
            cv_.wait(lock, [this] { return tasks_.size_approx() != 0 || stopped_; });
        }
    }
}

AeroQueue::~AeroQueue() {
    stop(true);
}