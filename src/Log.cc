#include "Log.hpp"
#include <chrono>
#include <ctime>
#include <sstream>
#include <stdexcept>
#include <iostream>

AsyncLog::AsyncLog()
    : running_(false), flushInterval_(3), buffer_(BUFFER_SIZE), nextBuffer_(BUFFER_SIZE) {
    buffer_.clear();   // size = 0, capacity = BUFFER_SIZE
    nextBuffer_.clear();
}

void AsyncLog::init(const std::string& filename, int flushInterval) {
    filename_ = filename;
    flushInterval_ = flushInterval;
    file_.open(filename, std::ios::app | std::ios::out);
    if (!file_.is_open()) {
        throw std::runtime_error("Log file open failed: " + filename);
    }
    running_ = true;
    thread_ = std::thread(&AsyncLog::threadFunc, this);
}

void AsyncLog::write(LogLevel level, const std::string& msg) {
    // 格式化日志行：时间 + 级别 + 消息
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::string levelStr;
    switch (level) {
        case LogLevel::DEBUG: levelStr = "[DEBUG]"; break;
        case LogLevel::INFO:  levelStr = "[INFO]";  break;
        case LogLevel::WARN:  levelStr = "[WARN]";  break;
        case LogLevel::ERROR: levelStr = "[ERROR]"; break;
    }

    std::stringstream ss;
    ss << std::ctime(&t) << " " << levelStr << " " << msg << "\n";
    std::string logLine = ss.str();

    std::unique_lock<std::mutex> lock(mutex_);
    // 如果当前缓冲区剩余空间不足，先交换
    if (buffer_.size() + logLine.size() > BUFFER_SIZE) {
        // 将当前缓冲区移到待写队列
        buffers_.push_back(std::move(buffer_));
        // 使用备用缓冲区作为新的当前缓冲区
        if (!nextBuffer_.empty()) {
            buffer_ = std::move(nextBuffer_);
        } else {
            buffer_.resize(BUFFER_SIZE);
            buffer_.clear();
        }
        // 通知后端线程有数据可写
        cond_.notify_one();
    }
    // 将日志行追加到当前缓冲区
    buffer_.insert(buffer_.end(), logLine.begin(), logLine.end());
}

void AsyncLog::stop() {
    running_ = false;
    cond_.notify_one();
    if (thread_.joinable()) {
        thread_.join();
    }
    // 刷盘所有剩余数据
    std::unique_lock<std::mutex> lock(mutex_);
    if (!buffer_.empty()) {
        file_.write(buffer_.data(), buffer_.size());
        buffer_.clear();
    }
    for (const auto& buf : buffers_) {
        if (!buf.empty()) {
            file_.write(buf.data(), buf.size());
        }
    }
    file_.flush();
    file_.close();
}

void AsyncLog::threadFunc() {
    while (running_) {
        std::vector<std::vector<char>> buffersToWrite;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            // 等待条件：有待写缓冲区 或 定时超时
            cond_.wait_for(lock, std::chrono::seconds(flushInterval_), [this]() {
                return !buffers_.empty() || !running_;
            });
            // 将当前缓冲区也加入待写队列（如果有内容）
            if (!buffer_.empty()) {
                buffers_.push_back(std::move(buffer_));
                if (!nextBuffer_.empty()) {
                    buffer_ = std::move(nextBuffer_);
                } else {
                    buffer_.resize(BUFFER_SIZE);
                    buffer_.clear();
                }
            }
            // 取出所有待写缓冲区
            buffersToWrite.swap(buffers_);
        }

        // 写入文件（无锁）
        for (const auto& buf : buffersToWrite) {
            if (!buf.empty()) {
                file_.write(buf.data(), buf.size());
            }
        }
        file_.flush();

        // 回收缓冲区，用于备用
        {
            std::unique_lock<std::mutex> lock(mutex_);
            for (auto& buf : buffersToWrite) {
                buf.clear();
                if (nextBuffer_.empty()) {
                    nextBuffer_ = std::move(buf);
                }
                // 否则丢弃（内存自动释放）
            }
        }
    }
}