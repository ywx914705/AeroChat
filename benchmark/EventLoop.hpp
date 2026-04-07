/*
EventLoop是AeroChat中Reactor模式的核心组件。
每个EventLoop 对象绑定一个线程,实现 "One Loop Per Thread" 设计。
它负责：
驱动事件循环调用Poller的poll方法获取活跃Channel；
分发 I/O 事件（通过 Channel 的回调）
执行跨线程投递的任务（通过无锁队列 pendingFunctors_）。
 */
#ifndef EVENTLOOP_HPP
#define EVENTLOOP_HPP

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <sys/eventfd.h>   // for eventfd
#include <unistd.h>         // for close

#include "Channel.hpp"
#include "Poller.hpp"
#include "concurrentqueue.hpp"

class EventLoop {
public:
  EventLoop();
  ~EventLoop();

  // 启动事件循环（阻塞）
  void loop();

  // 请求退出事件循环
  void quit();

  // 更新 Channel 在 Poller 中的注册状态（新增、修改或删除）
  void updateChannel(Channel *channel);
  void removeChannel(Channel *channel);

  // 跨线程任务投递
  void runInLoop(
      const std::function<void()> &cb); // 若在当前线程则直接执行，否则入队
  void queueInLoop(const std::function<void()> &cb); // 总是入队

  // 判断调用者是否在 EventLoop 所属线程中
  bool isInLoopThread() const {
    return threadId_ == std::this_thread::get_id();
  }

  // 索引（用于区分主 Reactor 和子 Reactor）
  int getIndex() const { return index_; }
  void setIndex(int idx) { index_ = idx; }

private:
  // 执行所有待处理的跨线程回调
  void doPendingFunctors();

  // 唤醒 EventLoop 线程（当有任务入队且不在本线程时调用）
  void wakeup();
  // eventfd 读事件回调，用于清空唤醒标志
  void handleWakeup();

  std::atomic<bool> looping_; // 是否正在循环
  std::atomic<bool> quit_;    // 是否退出
  int index_; // 标识（主 Reactor 为 0，子 Reactor 为 1..N）
  std::thread::id threadId_; // 所属线程 ID
  moodycamel::ConcurrentQueue<std::function<void()>>
      pendingFunctors_;         // 无锁队列，存储跨线程任务
  bool callingPendingFunctors_; // 是否正在执行待处理回调(防止重入)

  std::unique_ptr<Poller> poller_; // I/O多路复用器(当前为epoll)

  // 唤醒机制相关 
  int wakeupFd_;                               // eventfd 文件描述符
  std::unique_ptr<Channel> wakeupChannel_;     // 封装 wakeupFd_ 的 Channel
};

#endif // EVENTLOOP_HPP