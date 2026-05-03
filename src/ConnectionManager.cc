#include "ConnectionManager.hpp"
#include "Log.hpp"
#include <iostream>
#include <mutex>

const size_t MAX_PENDING_BYTES = 4 * 1024 * 1024; // 4MB
// ConnectionManager是一个全局单例
ConnectionManager &ConnectionManager::instance() {
  static ConnectionManager instance;
  return instance;
}

void ConnectionManager::addUser(int fd, std::shared_ptr<User> user) {
  // 增加一个新的用户/连接到连接管理器中
  if (!user)
    return;
  EventLoop *loop = user->getLoop(); // 获取用户所属的EventLoop指针
  if (!loop) {
    LOG_ERROR("[ConnectionManager] addUser: user has no loop, fd=" +
              std::to_string(fd));
    return;
  }

  std::unique_lock lock(mutex_);
  users_[fd] = user; // users_是一个unordered_map结构[fd,User]
  loopUsers_[loop].emplace_back(user); // 往对应loop的vector<User>插入这个用户
}

void ConnectionManager::removeUser(int fd) {
  std::unique_lock lock(mutex_);

  auto it = users_.find(fd);
  if (it == users_.end())
    return;

  auto user = it->second.lock();//通过fd获取对应的对象
  if (user) {
    EventLoop *loop = user->getLoop();
    if (loop) {
      auto lit = loopUsers_.find(loop);
      if (lit != loopUsers_.end()) {
        auto &vec = lit->second;
        // 从 vector 中移除该 user（通过 fd 匹配）
        //std::remove_if()有三个参数  起始迭代器 结束迭代器 谓词(判断是否需要删除),remove_if并不是真正意义上删除
        //元素,它只是将满足条件的元素移到容器末尾,并返回新的逻辑末尾,这样erase()就能删除返回新的尾到尾部的所有元素了
        vec.erase(std::remove_if(vec.begin(), vec.end(),
                                 [fd](const std::weak_ptr<User> &wp) {
                                   auto u = wp.lock();
                                   return u && u->fd() == fd;
                                 }),
                  vec.end());
        if (vec.empty())
          loopUsers_.erase(lit);//如果该EventLoop下不再有用户,则从 loopUsers_中删除该条目
      }
    }
  }
  users_.erase(it);
}

std::shared_ptr<User> ConnectionManager::getUser(int fd) {
  std::shared_lock lock(mutex_); // 使用读写锁保护
  auto it = users_.find(fd);     // 在Users_中查找这个fd
  if (it != users_.end()) {
    return it->second.lock(); // 如果找到了就返回这个fd所对应的用户,否则返回为空
    // 为什么调用weak_ptr::lock()?
    // 如果User对象仍然存在,lock()返回一个空的shared_prt,之后就可以安全使用User,否则User对象已经被销毁
    // lock()返回一个空的shared_ptr,我们就可以知道这个fd所对象的User已经失效了,避免访问已销毁内存！
  }
  return nullptr;
}

bool ConnectionManager::sendToUser(int fd, const std::string &message) {
    //向指定fd的用户发送一条消息,返回true表示此操作成功
  auto user = getUser(fd);//通过fd获取User对象
  if (!user)
    return false;//这个用户不存在直接返回false表明此操作失败了
  user->send(message.data(), message.size());//否则调用User::send()发送这个消息
  return true;//返回成功,表示此消息已成功发送
}

void ConnectionManager::broadcastMessage(const std::string &message) {
  // 1. 在锁内获取所有 loop 及其用户列表的快照(拷贝weak_ptr列表)
  std::unordered_map<EventLoop *, std::vector<std::weak_ptr<User>>>
      loopUsersCopy;
  {
    std::shared_lock lock(mutex_);
    for (auto &[loop, vec] : loopUsers_) {
      loopUsersCopy[loop] = vec; // 拷贝weak_ptr列表
    }
  }

  // 2. 对每个loop,将广播任务投递到其事件循环
  for (auto &[loop, weakVec] : loopUsersCopy) {
    if (!loop)
      continue;
    loop->queueInLoop([weakVec, message]() {//对每个 EventLoop,通过loop->queueInLoop投递一个lambda
                                            //任务,该lambda会在目标所属线程中执行
      for (auto &weak : weakVec) {
        auto user = weak.lock();
        if (user && user->pendingBytes() <= MAX_PENDING_BYTES) {// 背压控制
          user->send(message.data(), message.size());
        }
      }
    });
  }
}


std::vector<std::shared_ptr<User>> ConnectionManager::getUsers(const std::vector<int> &fds) {
    //根据一个fd列表,批量获取对应的User对象,以share_ptr的形式返回,这里不再进行解释了
  std::vector<std::shared_ptr<User>> result;
  result.reserve(fds.size());//预分配空间,防止多次扩容

  std::shared_lock lock(mutex_);//读锁,允许多个读操作并发
  for (int fd : fds) {
    auto it = users_.find(fd);
    if (it != users_.end()) {
      result.push_back(it->second.lock());
    } else {
      result.push_back(nullptr);
    }
  }
  return result;
}

std::unordered_map<EventLoop*, std::vector<int>> ConnectionManager::getIdleConnections(time_t timeout) const {
  std::unordered_map<EventLoop*, std::vector<int>> result;
  time_t now = time(nullptr);

  std::shared_lock lock(mutex_);
  for (auto& [loop, vec] : loopUsers_) {
    std::vector<int> idleFds;
    for (auto& weak : vec) {
      auto user = weak.lock();
      if (!user) continue;
      if (!user->isAlive() || now - user->lastActiveTime() > timeout) {
        idleFds.push_back(user->fd());
      }
    }
    if (!idleFds.empty()) {
      result[loop] = std::move(idleFds);
    }
  }
  return result;
}