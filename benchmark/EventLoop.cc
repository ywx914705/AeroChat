#include "EventLoop.hpp"
#include "Log.hpp"
#include <iostream>
#include <cstring>
#include <cerrno>
#include <sys/eventfd.h>
#include <unistd.h>

EventLoop::EventLoop()
    : looping_(false),
      quit_(false),
      index_(-1),
      callingPendingFunctors_(false),
      poller_(std::make_unique<Poller>(this))   // 创建Poller对象,传入this以便Poller内部可调用EventLoop方法(如需要)
{
    threadId_ = std::this_thread::get_id();

    // 创建eventfd用于跨线程唤醒
    wakeupFd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeupFd_ < 0) {
        LOG_ERROR("EventLoop::EventLoop() eventfd creation failed: " + std::string(strerror(errno)));
        abort();
    }

    // 将 wakeupFd_封装成Channel,并加入事件监听
    wakeupChannel_ = std::make_unique<Channel>(this, wakeupFd_);
    wakeupChannel_->setReadCallback([this]() { handleWakeup(); });
    wakeupChannel_->enableReading();   // 监听可读事件（当 eventfd 被写入时触发）
    updateChannel(wakeupChannel_.get());

    LOG_INFO("EventLoop created with wakeupFd=" + std::to_string(wakeupFd_));
}

EventLoop::~EventLoop() {
    quit();               //设置退出标志
    if (wakeupFd_ >= 0) {
        close(wakeupFd_);
    }
    // poller_的析构会自动关闭epollfd_,无需额外操作
}

void EventLoop::loop() {
    looping_ = true;
    quit_ = false;

    // 存放每次epoll_wait返回的活跃Channel列表
    std::vector<Channel*> activeChannels;

    while (!quit_) {
        activeChannels.clear();

        // 调用Poller的poll方法,阻塞等待事件。
        // 超时设为-1(无限等待),因为唤醒机制(eventfd)能随时唤醒线程。
        // 若有任务通过queueInLoop入队,wakeup()会向eventfd写入数据,epoll立即返回。
        poller_->poll(-1, &activeChannels);

        // 处理 I/O 事件
        for (Channel* channel : activeChannels) {
            channel->handleEvent();   // 根据 revents_调用相应的回调
        }

        // 处理跨线程任务(如其他线程投递的数据库操作结果)
        doPendingFunctors();
    }

    looping_ = false;
}

void EventLoop::updateChannel(Channel* channel) {
    poller_->updateChannel(channel);
}

void EventLoop::removeChannel(Channel* channel) {
    poller_->removeChannel(channel);
}

void EventLoop::runInLoop(const std::function<void()>& cb) {
    if (isInLoopThread()) {
        cb();   //当前线程直接执行
    } else {
        queueInLoop(cb);
    }
}

void EventLoop::queueInLoop(const std::function<void()>& cb) {
    pendingFunctors_.enqueue(cb);
    // 如果当前线程不是本 EventLoop线程,则需要唤醒线程(因为可能正阻塞在epoll_wait中)
    if (!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::doPendingFunctors() {
    callingPendingFunctors_ = true;
    std::function<void()> functor;
    while (pendingFunctors_.try_dequeue(functor)) {
        functor();
    }
    callingPendingFunctors_ = false;
}

void EventLoop::quit() {
    quit_ = true;
    // 如果 quit_ 被其他线程设置,且本线程正阻塞在epoll_wait,需要唤醒以便退出循环
    if (!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = write(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        LOG_ERROR("EventLoop::wakeup() writes " + std::to_string(n) + " bytes instead of 8");
    }
}

void EventLoop::handleWakeup() {
    uint64_t one;
    ssize_t n = read(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        LOG_ERROR("EventLoop::handleWakeup() reads " + std::to_string(n) + " bytes instead of 8");
    }
    // 注意：这里不需要做其他事情,仅仅是为了清除eventfd的内容,避免无限触发。
    // 真正的跨线程任务在 doPendingFunctors() 中执行。
}