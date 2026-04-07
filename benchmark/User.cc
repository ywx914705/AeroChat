#include "User.hpp"
#include "Channel.hpp"
#include "EventLoop.hpp"
#include "Log.hpp"
#include <unistd.h>
#include <sys/socket.h>
#include <errno.h>
#include <fcntl.h>
#include <algorithm>
#include <cstring>

User::User(int fd, EventLoop* loop)
    : fd_(fd), loop_(loop), channel_(std::make_unique<Channel>(loop, fd)) {
    writeBuf_.reserve(8192);
    readBuf_.reserve(65536);
}

void User::start() {
    channel_->setReadCallback([self = shared_from_this()] { self->handleRead(); });
    channel_->setWriteCallback([self = shared_from_this()] { self->handleWrite(); });
    channel_->enableReading();
    channel_->enableET();
    loop_->updateChannel(channel_.get());
}

void User::handleRead() {
    if (!isAlive_) return;

    char extrabuf[65536];
    ssize_t n;
    while (true) {
        n = read(fd_, extrabuf, sizeof(extrabuf));
        if (n > 0) {
            readBuf_.insert(readBuf_.end(), extrabuf, extrabuf + n);
        } else if (n == 0) {
            handleError();
            return;
        } else {
            if (errno == EAGAIN) break;
            handleError();
            return;
        }
    }

    if (!readBuf_.empty()) {
        // 按 '\n' 切分消息（也可以不做切分，直接整包 Echo，但保留逻辑）
        char* start = readBuf_.data();
        char* end = start + readBuf_.size();
        char* line_end;
        while ((line_end = std::find(start, end, '\n')) != end) {
            size_t len = line_end - start + 1;
            std::string line(start, len);
            if (messageCb_) {
                messageCb_(fd_, line, shared_from_this());
            }
            start = line_end + 1;
        }
        if (start > readBuf_.data()) {
            size_t remaining = end - start;
            if (remaining > 0) {
                memmove(readBuf_.data(), start, remaining);
            }
            readBuf_.resize(remaining);
        }
    }
}

void User::handleWrite() {
    if (!isAlive_) return;
    size_t totalSent = 0;
    while (writePos_ < writeBuf_.size() && totalSent < 65536) {
        ssize_t n = write(fd_, writeBuf_.data() + writePos_, writeBuf_.size() - writePos_);
        if (n > 0) {
            writePos_ += n;
            totalSent += n;
        } else if (n == -1 && errno == EAGAIN) {
            break;
        } else {
            handleError();
            return;
        }
    }
    if (writePos_ == writeBuf_.size()) {
        writePos_ = 0;
        writeBuf_.clear();
        channel_->disableWriting();
        loop_->updateChannel(channel_.get());
    } else {
        channel_->enableWriting();
        loop_->updateChannel(channel_.get());
    }
}

void User::send(const char* data, size_t len) {
    if (!isAlive_ || len == 0) return;
    if (loop_->isInLoopThread()) {
        if (writePos_ == writeBuf_.size()) {
            ssize_t n = write(fd_, data, len);
            if (n > 0) {
                if (static_cast<size_t>(n) == len) return;
                data += n;
                len -= n;
            } else if (n == -1 && errno != EAGAIN) {
                handleError();
                return;
            }
        }
        if (len > 0) {
            writeBuf_.insert(writeBuf_.end(), data, data + len);
            if (!channel_->isWriting()) {
                channel_->enableWriting();
                loop_->updateChannel(channel_.get());
            }
        }
    } else {
        std::string msg(data, len);
        loop_->queueInLoop([self = shared_from_this(), msg] {
            self->send(msg.data(), msg.size());
        });
    }
}

void User::handleError() {
    if (closing_) return;
    closing_ = true;
    isAlive_ = false;
    if (errorCb_) errorCb_(fd_);
    channel_->disableAll();
    loop_->removeChannel(channel_.get());
    close(fd_);
}