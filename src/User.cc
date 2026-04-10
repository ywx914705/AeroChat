#include "User.hpp"
#include "Channel.hpp"
#include "EventLoop.hpp"
#include "Log.hpp"
#include <algorithm>
#include <cstring>
#include <ctime>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

User::User(int fd, EventLoop *loop)
    : fd_(fd), loop_(loop),
      channel_(std::make_unique<Channel>(
          loop, fd)), // 创建Channel对象,将fd与EventLoop进行绑定
      readBuf_(), writeBuf_(), writePos_(0), userId_(-1), isLoggedIn_(false),
      isAlive_(true), closing_(false), lastActive_(time(nullptr)),
      messageCb_(nullptr), errorCb_(nullptr) {
  writeBuf_.reserve(8192); // 提前分配8kb空间
  int flags = fcntl(fd_, F_GETFL, 0);
  fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
  // 设置发送/接收缓冲区大小,优化网络性能
  int sndbuf = 16384, rcvbuf = 32768;
  setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
  setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
}
User::~User(){
  if (fd_ >= 0) {
     ::close(fd_);
      LOG_DEBUG("[User] FD " + std::to_string(fd_) + " closed in destructor");
    // 注意：fd_ 是 const int，不能赋值为 -1，但 close 后不再使用即可
   }
}
void User::setLoggedIn(bool status) {
  isLoggedIn_.store(status, std::memory_order_relaxed);
  updateActiveTime();
}

bool User::isLoggedIn() const {
  return isLoggedIn_.load(std::memory_order_relaxed);
}

void User::setUserId(int id) {
  userId_ = id;
  updateActiveTime();
}

// 返回用户ID
int User::getUserId() const { return userId_; }

// 设置消息回调
void User::setMessageCallback(MessageCallback cb) {
  messageCb_ = std::move(cb);
}

// 设置错误回调
void User::setErrorCallback(ErrorCallback cb) { errorCb_ = std::move(cb); }

void User::start() { // 注册到事件循环
  channel_->setReadCallback(
      [self = shared_from_this()]() { self->handleRead(); });
  channel_->setWriteCallback(
      [self = shared_from_this()]() { self->handleWrite(); });
  // 将handleRead和handleWrite绑定到Channel的读写回调中,shared_from_this()返回的shared_ptr,确保在回调执行时User对象仍然存活
  channel_->enableReading(); // 设置events_|=EPOLLIN,告诉内核监听这个fd的读事件
  channel_->enableET();
  loop_->updateChannel(
      channel_.get()); // 更新到EventLoop->Poller::updateChanneel->epoll_ctl()
}

void User::updateActiveTime() { lastActive_ = time(nullptr); }//更新活动时间

//收到数据调用handleRead,服务器这里并没有像传统服务器一样主动调用recv接收消息,而是使用了epoll,可以认为是“当哪个连接有消息，
//就自动回调该连接的 handleRead
void User::handleRead() {
  if (!isAlive_)
    return;           // 连接已关闭，不再处理读事件
  updateActiveTime(); // 更新最后活动时间

  char extrabuf[65536]; // 临时缓冲区
  ssize_t n = 0;

  // ET 模式必须循环读取直到 EAGAIN            否则会丢数据
  while (true) {
    n = read(fd_, extrabuf, sizeof(extrabuf));
    if (n > 0) {
      // 容量检查，防止恶意大包
      if (readBuf_.size() + static_cast<size_t>(n) > MAX_READ_BUFFER) {
        LOG_ERROR("[User] Read buffer overflow, fd=" + std::to_string(fd_));
        handleError();
        return;
      }
      // 预分配空间，减少扩容
      readBuf_.reserve(readBuf_.size() + n);
      readBuf_.insert(readBuf_.end(), extrabuf, extrabuf + n);
    } else if (n == 0) {
      LOG_WARN("[User] peer closed connection fd=" +
               std::to_string(fd_)); // 对端关闭连接
      handleError();
      return;
    } else {
      if (errno == EAGAIN) {
	  	//ET模式下必须读到EAGAGIN否则会造成数据丢失
        break; // 数据已读完
      } else {
        LOG_ERROR("[User] read error fd=" + std::to_string(fd_) + " errno=" +
                  std::to_string(errno) + " (" + strerror(errno) + ")");
        handleError();
        return;
      }
    }
  }

  if (!readBuf_.empty()) {
    processReadBuffer(); // 解析完整消息
  }
}

//AeroChat是如何解决消息粘包的问题的？
//通过\n进行分离,当读到\n的时候就认为已经读取到了完整的消息
void User::processReadBuffer() {
  char *start = readBuf_.data();       // 当前待处理数据的起始位置
  char *end = start + readBuf_.size(); // 缓冲区的末尾（结束位置）

  char *line_end; // 指向找到的 '\n' 位置

  // 循环查找 '\n'，直到没有更多换行符
  while ((line_end = std::find(start, end, '\n')) != end) {
    size_t len = line_end - start + 1; // 一条完整消息的长度（包括换行符）
    std::string line(start, len); // 构造消息字符串（包含换行符）
    if (messageCb_) {
      messageCb_(fd_, line); // 调用用户回调,处理完整消息
    }
    start = line_end + 1; // 移动指针到下一个可能消息的起始位置
  }

  // 处理剩余未完成的数据
  if (start > readBuf_.data()) {    // 如果处理过至少一条消息
    size_t remaining = end - start; // 剩余未处理的数据长度
    if (remaining > 0) {
      // 将剩余数据移动到缓冲区开头
      memmove(readBuf_.data(), start, remaining);
    }
    readBuf_.resize(remaining); // 调整缓冲区大小为剩余数据长度
  }
}

void User::handleWrite() {
  if (!isAlive_)
    return; // 连接已关闭，不再处理写事件
  updateActiveTime();
  size_t totalSent = 0;
  while (writePos_ < writeBuf_.size() && totalSent < MAX_WRITE_PER_LOOP) {
    ssize_t n =
        write(fd_, writeBuf_.data() + writePos_, writeBuf_.size() - writePos_);
    if (n > 0) {
      writePos_ += n;
      totalSent += n;
    } else if (n == -1 && errno == EAGAIN) {
      break; // 缓冲区写满,等待下次可写事件
    } else {
      if (n == -1) {
        LOG_ERROR("[User] write error fd=" + std::to_string(fd_) +
                  " errno=" + std::to_string(errno) + " (" +
                  std::string(strerror(errno)) + ")");
      } else {
        LOG_ERROR("[User] write returned " + std::to_string(n) +
                  " (should not happen)");
      }
      handleError(); // 写错误
      return;
    }
  }
  if (writePos_ == writeBuf_.size()) { // 数据全部发送完毕
    writePos_ = 0;
    writeBuf_.clear();
    if (writeBuf_.capacity() > 16384) { // 写缓冲区过大,释放内存,然后重新分配
      std::vector<char>().swap(writeBuf_);
      writeBuf_.reserve(8192);
    }
    channel_->disableWriting();
    loop_->updateChannel(channel_.get());
  } else {
    channel_->enableWriting();
    loop_->updateChannel(channel_.get());
  }
}

void User::handleError() {
  bool expected = false;
  if (!closing_.compare_exchange_strong(expected, true)) {
    return;
  }
  isAlive_ = false;
  int err = errno;
  LOG_ERROR("[User] handleError fd=" + std::to_string(fd_) + " errno=" +
            std::to_string(err) + " (" + std::string(strerror(err)) + ")");

  // 先调用回调（此时 fd 仍有效）
  if (errorCb_) {
    errorCb_(fd_);
  }

  // 再移除事件并关闭 fd
  channel_->disableAll();
  loop_->removeChannel(channel_.get());
  close(fd_);
  // 注意：fd_ 是 const，不能赋值为 -1，但已关闭，后续不应再使用 fd_，依靠
  // isAlive_ 标志即可。
}
/*
User::send()中
write返回值
n > 0 且 n == len：全部发送成功,更新活动时间后返回。

n > 0 但 n < len：部分发送成功,调整data和len指向剩余部分,继续执行后续缓冲逻辑。

n == -1 且 errno != EAGAIN：发生不可恢复错误(如连接断开调用),handleError()
关闭连接。

n == -1 且 errno ==
EAGAIN：发送缓冲区已满,本次无法发送任何数据。此时data和len保持不变,后续会进入缓冲逻辑。

*/
void User::send(const char *data, size_t len) {
  // 进行一个检查
  if (!isAlive_ || len == 0)return;
  if (loop_->isInLoopThread()) { // 判断当前线程是否为该User所属的Reactor线程,如果是则直接操作fd
    if (writePos_ == writeBuf_.size()) {
     // writeBuf中没有待发送的数据(因为writePos_指向已发送位置,等于总长度时说明全部发送完毕)
      ssize_t n = write(fd_, data, len);
      if (n > 0) {
        if (static_cast<size_t>(n) == len) {
          updateActiveTime();
          return;
        }
        data += n;
        len -= n;
      } else if (n == -1 && errno != EAGAIN) {
        handleError();
        return;
      }
    }
    if (len > 0) {
      const size_t maxWriteBuffer = 4 * 1024 * 1024; //4MB;
      if (writeBuf_.size() + len > maxWriteBuffer) {
	  	//背压保护,大于4MB的时候直接关闭连接防止内存耗尽
        LOG_ERROR("[User] send buffer full, closing connection fd=" + std::to_string(fd_));
        handleError();
        return;
      }
      writeBuf_.insert(writeBuf_.end(), data, data + len);
      if (!channel_->isWriting()) {
        channel_->enableWriting();
        loop_->updateChannel(channel_.get());
      }
    }
    updateActiveTime();
  } 
    else { // 如果当前线程不是User所属线程,不能直接操作fd,所以直接打包成一个lambda函数,通过queueInLoop投递到所属线程的事件循环中
    std::string msg(data, len);
    loop_->queueInLoop([self = shared_from_this(), msg]() {
      self->send(msg.data(), msg.size());
    });
  }
}