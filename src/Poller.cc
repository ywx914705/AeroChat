#include "Poller.hpp"
#include "Channel.hpp"
#include "EventLoop.hpp"
#include "Log.hpp"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>
// Poller是对linux中epoll_create(),epoll_ctl(),以及epoll_wait()的高级封装
Poller::Poller(EventLoop *loop)
    : ownerLoop_(loop),
      epollfd_(epoll_create1(EPOLL_CLOEXEC)), // 调用epoll_create1()创建fd
      events_(1024) {
  if (epollfd_ < 0) {
    LOG_ERROR("epoll_create1 failed: " + std::string(strerror(errno)));
    abort();
  }
}
Poller::~Poller(){
   if(epollfd_>=0)
   close(epollfd_);
}

void Poller::updateChannel(Channel *channel) {
  if (!channel || channel->fd() < 0) {
    LOG_ERROR("Poller::updateChannel: invalid channel/fd");
    return;
  }

  int fd = channel->fd();
  if (fcntl(fd, F_GETFD) == -1 && errno == EBADF) {
    LOG_ERROR("Poller::updateChannel: fd " + std::to_string(fd) +
              " is invalid, skipping");
    channels_.erase(fd);
    if (channel) {
      channel->setIndex(kNew);
    }
    return;
  }

  struct epoll_event event;
  memset(&event, 0, sizeof(event));
  event.events = channel->events();
  event.data.ptr = channel;

  int channelIndex = channel->index();
  if (channelIndex == kNew) {
    if (epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &event) < 0) {
      if (errno != EEXIST) {
        LOG_ERROR("epoll_ctl ADD failed (fd=" + std::to_string(fd) +
                  "): " + std::string(strerror(errno)));
      } else {
        epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &event);
      }
    } else {
      channels_[fd] = channel;
      channel->setIndex(kAdded);
    }
  } else if (channelIndex == kAdded) {
    if (channel->events() == 0) {
      if (epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, &event) < 0) {
        if (errno != ENOENT) {
          LOG_ERROR("epoll_ctl DEL failed (fd=" + std::to_string(fd) +
                    "): " + std::string(strerror(errno)));
        }
      }
      channels_.erase(fd);
      channel->setIndex(kNew);
    } else {
      if (epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &event) < 0) {
        LOG_ERROR("epoll_ctl MOD failed (fd=" + std::to_string(fd) +
                  "): " + std::string(strerror(errno)));
        channels_.erase(fd);
        channel->setIndex(kNew);
      }
    }
  } else {
    LOG_ERROR("Poller::updateChannel: invalid index for fd=" +
              std::to_string(fd));
  }
}

void Poller::removeChannel(Channel *channel) {
  if (!channel || channel->fd() < 0)
    return;
  int fd = channel->fd();

  if (fcntl(fd, F_GETFD) == -1 && errno == EBADF) {
    channels_.erase(fd);
    channel->setIndex(kNew);
    return;
  }
  if (channels_.count(fd)) {
    if (epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, nullptr) < 0 &&
        errno != ENOENT) {
      LOG_ERROR("epoll_ctl DEL failed (fd=" + std::to_string(fd) +
                "): " + std::string(strerror(errno)));
    }
    channels_.erase(fd);
    channel->setIndex(kNew);
  }
}

void Poller::poll(int timeoutMs, std::vector<Channel *> *activeChannels) {
  // 底层就是调用epoll_wait()
  int numEvents = epoll_wait(epollfd_, events_.data(),
                             static_cast<int>(events_.size()), timeoutMs);
  int savedErrno = errno;

  if (numEvents > 0) {
    fillActiveChannels(numEvents, activeChannels);
    if (static_cast<size_t>(numEvents) == events_.size()) {
      events_.resize(events_.size() * 2);
    }
  } else if (numEvents == 0) {
  } else {
    if (savedErrno != EINTR) {
      errno = savedErrno;
      LOG_ERROR("Poller::poll() error: " + std::string(strerror(errno)));
    }
  }
}

void Poller::fillActiveChannels(int numEvents,
                                std::vector<Channel *> *activeChannels) const {
  for (int i = 0; i < numEvents; ++i) {
    Channel *channel = static_cast<Channel *>(events_[i].data.ptr);
    if (!channel)
      continue;
    if (channel->fd() <= 0 ||
        (fcntl(channel->fd(), F_GETFD) == -1 && errno == EBADF)) {
      continue;
    }
    channel->setRevents(events_[i].events);
    activeChannels->push_back(channel);
  }
}