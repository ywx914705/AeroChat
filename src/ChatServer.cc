#include "chatserver.hpp"
#include "Channel.hpp"
#include "ConnectionManager.hpp"
#include "ConnectionPool.hpp"
#include "EventLoop.hpp"
#include "GroupManager.hpp"
#include "Log.hpp"
#include "MessageRouter.hpp"
#include "MessageStore.hpp"
#include "RedisClient.hpp"
#include "SessionManager.hpp"
#include "Sock.hpp"
#include "User.hpp"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <arpa/inet.h>
#include <iostream>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/timerfd.h>

const int MAX_CONNECTIONS = 25000; // AeroChat接收的最大连接数 (8GB内存安全上限)
const int SOCKET_SNDBUF = 262144; // socket发送缓冲区的大小
const int SOCKET_RCVBUF = 262144; // socket接收缓冲区的大小
const int DB_POOL_SIZE = 500;     // MySQL连接池的大小  根据内存调整
const int SUB_REACTOR_NUM =std::max(4, static_cast<int>(std::thread::hardware_concurrency()));

// 线程数由CPU核数所决定
std::atomic<int> ChatServer::connCount_(0);
// 为什么使用原子变量？
// 原子操作在x86上会翻译成lock前缀的指令,直接操作缓存行,比加锁快很多,这里是为了性能考虑选择原子变量
// 记录连接数

ChatServer::ChatServer(EventLoop *loop, uint16_t port, int numSubReactors)
    : loop_(loop), port_(port),
      threadPool_(new EventLoopThreadPool(
          loop, numSubReactors > 0 ? numSubReactors : SUB_REACTOR_NUM)),
      listenFd_(-1), acceptChannel_(nullptr), sock_(), epollTimeout_(100),
      timerFd_(-1) {
  // 1、设置信号处理(忽略SIGPIPE,处理SIGSEGV)
  signal(SIGPIPE, SIG_IGN);
  signal(SIGABRT, SIG_IGN);
  signal(SIGSEGV, [](int) {
    LOG_ERROR("SEGV caught");
    exit(1);
  });
  // 2、提高文件描述符限制(支持更多并发连接)
  struct rlimit rlim;
  if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
    rlim.rlim_cur = MAX_CONNECTIONS + 1000;
    rlim.rlim_max = MAX_CONNECTIONS + 1000;
    setrlimit(RLIMIT_NOFILE, &rlim);
  }
  // 3、初始化数据库连接池、DBManager、SessionManager
  ConnectionPool::getInstance().init("localhost", "root", "123456", "chat_db",3306, DB_POOL_SIZE);
  DBManager::getInstance().connect("localhost", "root", "123456", "chat_db",3306);
  SessionManager::instance().initEmpty();

  int totalThreads = SUB_REACTOR_NUM + 1; // 主 + 子
  perLoopUsers_.resize(totalThreads); // perLoopUsers_是一个vector结果,使用resize来分配空间
  // 每个线程都创建一个perLoopUsers,从名字上就能看出来,一个User一个Loop
  for (int i = 0; i < totalThreads; ++i) {
    perLoopUsers_[i] = std::make_unique<PerLoopUsers>();
    perLoopUsers_[i]->map.reserve(MAX_CONNECTIONS / totalThreads + 100);
    // 此项目中这里最大连接数是1.6W/(线程数+100)
    // +100是一个安全缓冲,防止因分配不均导致实际连接数 超过均值
  }
  GroupManager::instance().initPresetGroups(); // 初始化8个预设群组
  GroupManager::instance().loadAllGroupsFromDB();
  LOG_INFO("ChatServer初始化完成");
}

ChatServer::~ChatServer() {
  if (listenFd_ >= 0)
    close(listenFd_); // 关闭监听 socket和timerfd,释放资源。
  if (timerFd_ >= 0)
    close(timerFd_);
  ConnectionPool::getInstance().close(); // 关闭数据库连接池，释放所有连接。

  // 先停止 Redis 订阅（这会唤醒并退出订阅线程）
  RedisClient::instance().unsubscribe();

  // 再等待我们自己的线程结束
  redisSubRunning_ = false;
  if (redisSubThread_ && redisSubThread_->joinable()) {
    redisSubThread_->join();
  }
}

void ChatServer::start() {

  // 1、启动线程池
  threadPool_->start();
  auto loops = threadPool_->getAllLoops(); // 获取所有线程的Loop指针
  if (loops.size() + 1 > perLoopUsers_.size()) {
    perLoopUsers_.resize(loops.size() + 1);
    for (size_t i = 0; i < perLoopUsers_.size(); ++i) {
      if (!perLoopUsers_[i]) {
        perLoopUsers_[i] = std::make_unique<PerLoopUsers>();
      } // 为每个线程的 map 预分配内存,避免运行时rehash。每个线程平均处理
        // MAX_CONNECTIONS / totalThreads 个连接, 加 100 作为安全缓冲。
      perLoopUsers_[i]->map.reserve(MAX_CONNECTIONS / (loops.size() + 1) + 100);
    }
  }

  // 2、创建监听socket
  listenFd_ = sock_.Socket();
  int reuse = 1;
  setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &reuse,sizeof(reuse)); // SO_REUSEADDR 和 SO_REUSEPORT 允许快速重启，避免端口占用。
  setsockopt(listenFd_, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse));
  fcntl(listenFd_, F_SETFL,O_NONBLOCK |FD_CLOEXEC); // 将其设计为非阻塞模式,(非阻塞+IO多路复用)配合epoll ET
  sock_.Bind(listenFd_, port_); // 绑定端口
  sock_.Listen(listenFd_, 8192); // 底层就是调用linux中的listen()
  // 这里需要注意,服务器端必须调用listen(),客户端才能与服务器进行三次握手,然后建立连接

  // 3、创立监听Channel监听socket(由socket()、bind()、listen() 共用)和
  // accept()返回的连接socket,
  // 是两个不同的fd,而这里创建的acceptChannel_是对监听socket的封装。
  acceptChannel_ = std::make_unique<Channel>(loop_,listenFd_);

  //创建Channel对象,封装listenFd_,并将其与主EventLoop绑定
  acceptChannel_->setReadCallback([this] {
    handleAccept();
  }); 
  // 设置读回调函数,当listenFd_有可读事件(新连接到来)时Channel就会调用
  // 这个回调 :handleAccept() 用于接收新连接
  acceptChannel_->enableReading(); // 将Channel关注的事件设置为EPOLLIN,并调用update()通知EventLoop同步到对应的Poller
  acceptChannel_->enableET(); // 设置为ET模式
  loop_->updateChannel(acceptChannel_.get());
  // 将Channel的变化同步到EventLoop中,最终由EventLoop通过其内部的Poller调用epoll_ctl,将listenFd_及其关注的事件(EPOLLIN|EPOLLET)添加到
  // epoll实例中,至此:主Reactor(MainReactor)开始监听该fd,等待新连接

  // 4、创建定时器
  timerFd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (timerFd_ < 0) {
    LOG_ERROR("timerfd_create failed");
    exit(1);
  }
  struct itimerspec howlong;
  bzero(&howlong, sizeof(howlong));
  howlong.it_value.tv_sec = 60;
  howlong.it_interval.tv_sec = 60;
  timerfd_settime(timerFd_, 0, &howlong, nullptr);

  timerChannel_ = std::make_unique<Channel>(loop_, timerFd_);
  timerChannel_->setReadCallback([this] { checkIdleConnections(); });
  timerChannel_->enableReading();
  loop_->updateChannel(timerChannel_.get());

  // [ADDED] 启动Redis订阅线程，用于接收在线状态变更推送
  redisSubRunning_ = true;
  redisSubThread_ =std::make_unique<std::thread>(&ChatServer::redisSubLoop, this);
  LOG_INFO("Redis subscription thread started");
  LOG_INFO("Server started on port " + std::to_string(port_));
}

// 主Reactor:只负责接受连接,然后把连接选择一个子Reactor(对应一个子线程),并将后续初始化任务投递到该子Reactor的EventLoop中,这就是主从
// Reactor的关键所在,主线程只做最轻量的工作(接收新连接然后分发)
//监听fd也就是ListenFd可读意味着有新连接到来  ListenFd是listen()这个函数所返回的fd
//accept所返回的fd是普通的fd,注意普通fd与监听fd的区分
void ChatServer::handleAccept() {
  while (true) {
    std::string clientIp;
    uint16_t clientPort;
    int connFd = sock_.Accept(listenFd_, &clientIp, &clientPort); // 底层就是调用linux中的accept函数
    if (connFd < 0) {
      if (errno == EAGAIN)
        break;
      LOG_ERROR("Accept error");
      break;
    }
    if (connCount_ >= MAX_CONNECTIONS) {
      close(connFd);
      continue;
    }
    EventLoop *subLoop = threadPool_->getNextLoop();
    subLoop->runInLoop([this, connFd, subLoop, clientIp, clientPort] {
      handleNewConn(connFd, subLoop, clientIp, clientPort);
    });
  }
}

// 此函数在子Reactor中执行,用于初始化一个新建立的客户端连接,它负责创建User对象,设置回调、注册到事件循环
void ChatServer::handleNewConn(int connFd, EventLoop *subLoop,
                               const std::string &clientIp,
                               uint16_t clientPort) {
  // 这个connFd是由accept所返回的fd,已自动设置为非阻塞和FD_CLOEXEC,subLoop为负责
  // 处理该连接的Rector的EventLoop指针,
  // 所有该连接的IO事件和业务逻辑都将在此EventLoop执行 设置 socket
  // 选项来优化网络性能
  (void)clientIp;
  (void)clientPort; // 添加这一行
  int sndbuf = SOCKET_SNDBUF, rcvbuf = SOCKET_RCVBUF;
  setsockopt(connFd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
  setsockopt(connFd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  int nodelay = 1;
  setsockopt(connFd, IPPROTO_TCP, TCP_NODELAY, &nodelay,sizeof(nodelay)); // 禁用Nagle算法
  int flags = fcntl(connFd, F_GETFL, 0);
  fcntl(connFd, F_SETFL, flags | O_NONBLOCK | FD_CLOEXEC);

  auto user = std::make_shared<User>(connFd, subLoop); // 创建User对象
  int idx = subLoop->getIndex();
  if (idx < 0 || static_cast<size_t>(idx) >= perLoopUsers_.size() ||
      !perLoopUsers_[idx]) {
    LOG_ERROR("handleNewConn: invalid index " + std::to_string(idx));
    close(connFd);
    return;
  }

  ConnectionManager::instance().addUser(connFd, user);

  user->setMessageCallback([](int fd, const std::string &msg) {
    auto u = ConnectionManager::instance().getUser(fd);
    if (u)
      MessageRouter::instance().onMessage(fd, msg, u);
  });

  user->setErrorCallback([this, idx](int fd) {
    // 先调用 SessionManager 登出，清除 Redis 中的在线状态
    SessionManager::instance().logout(fd);
    ConnectionManager::instance().removeUser(fd);
    removeUser(fd);
  });

  user->start();

  {
    std::unique_lock lock(perLoopUsers_[idx]->mutex);
    perLoopUsers_[idx]->map[connFd] = user;
  }
  connCount_++;

  rapidjson::Document welcome;
  welcome.SetObject();
  auto &alloc = welcome.GetAllocator();
  welcome.AddMember("type", "welcome", alloc);
  std::string msg =
      "Connected to chat server (fd:" + std::to_string(connFd) + ")";
  welcome.AddMember("msg", rapidjson::StringRef(msg.c_str()), alloc);
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  welcome.Accept(writer);
  std::string w = std::string(buf.GetString()) + "\n";
  user->send(w.data(), w.size());
}

void ChatServer::removeUser(int fd) {
  int targetIdx = -1;
  for (size_t i = 0; i < perLoopUsers_.size(); ++i) {
    if (!perLoopUsers_[i])
      continue;
    std::shared_lock lock(perLoopUsers_[i]->mutex);
    if (perLoopUsers_[i]->map.count(fd)) {
      targetIdx = i;
      break;
    }
  }
  if (targetIdx == -1)
    return;

  auto loops = threadPool_->getAllLoops();
  EventLoop *targetLoop = (targetIdx == 0) ? loop_ : loops[targetIdx - 1];
  targetLoop->runInLoop([this, targetIdx, fd] {
    if (!perLoopUsers_[targetIdx])
      return;
    std::unique_lock lock(perLoopUsers_[targetIdx]->mutex);
    perLoopUsers_[targetIdx]->map.erase(fd);
    connCount_--;
  });
}

void ChatServer::checkIdleConnections() {
  uint64_t exp;
  ssize_t nr = read(timerFd_, &exp, sizeof(exp));
  (void)nr;

  time_t now = time(nullptr);
  for (auto &perLoop : perLoopUsers_) {
    if (!perLoop)
      continue;
    std::vector<int> toClose;
    {
      std::shared_lock lock(perLoop->mutex);
      for (const auto &[fd, user] : perLoop->map) {
        if (!user->isAlive()) {
          toClose.push_back(fd);
          continue;
        }
        if (now - user->lastActiveTime() > kIdleTimeoutSeconds) {
          toClose.push_back(fd);
        }
      }
    }
    for (int fd : toClose) {
      std::shared_lock lock(perLoop->mutex);
      auto it = perLoop->map.find(fd);
      if (it != perLoop->map.end()) {
        auto user = it->second;
        user->getLoop()->runInLoop([user] { user->handleError(); });
      }
    }
  }
}

// [ADDED] Redis订阅线程函数，监听在线状态更新频道，并广播给所有客户端
void ChatServer::redisSubLoop() {
  RedisClient::instance().subscribe(
      "online_updates",
      [this](const std::string &msg) { broadcastOnlineUpdate(msg); });
}

// [ADDED] 将在线状态变更消息广播给所有在线客户端
void ChatServer::broadcastOnlineUpdate(const std::string &msg) {
  // 消息已包含完整 JSON，直接广播（带换行符，符合客户端协议）
  ConnectionManager::instance().broadcastMessage(msg + "\n");
}