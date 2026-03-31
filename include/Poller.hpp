/*
Poller:每个EventLoop拥有一个Poller对象(一一对应),负责实际的epoll操作,EventLoop通过updateChannel
将Channel的变化同步到Poller,并在每次循环中调用poll获取活跃的Channel,然后调用它们的handleEvent
*/
#pragma once
#include "noncopyable.hpp"
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>
// 前置声明
class EventLoop;
class Channel;

class Poller : noncopyable {
public:
  explicit Poller(EventLoop *loop);
  ~Poller() = default;

  void poll(int timeoutMs, std::vector<Channel *> *activeChannels);
  void updateChannel(Channel *channel);
  void removeChannel(Channel *channel);

private:
  void fillActiveChannels(int numEvents,
                          std::vector<Channel *> *activeChannels) const;

  EventLoop *ownerLoop_; // 对应的EventLoop
  int epollfd_;          // epoll_create1()创建出来的df
  std::unordered_map<int, Channel *> channels_; // fd与Channel的映射关系
  std::vector<struct epoll_event> events_; // 用于 epoll_wait 的事件数组
};