#pragma once
#include <atomic>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <condition_variable>

// 日志级别
enum class LogLevel { DEBUG, INFO, WARN, ERROR };

// 高性能异步日志（双缓冲区 + 条件变量）
class AsyncLog {
public:
    static AsyncLog &instance() {
        static AsyncLog inst;
        return inst;
    }

    // 初始化日志文件，flushInterval秒强制刷盘
    void init(const std::string &filename, int flushInterval = 3);
    // 写入日志（前端）
    void write(LogLevel level, const std::string &msg);
    // 停止日志线程
    void stop();

private:
    AsyncLog();
    ~AsyncLog() { stop(); }

    // 日志线程入口（后端刷盘）
    void threadFunc();

    std::atomic<bool> running_;
    int flushInterval_;                     // 强制刷盘间隔（秒）
    std::string filename_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;           // 通知后端线程
    std::vector<char> buffer_;               // 当前写入缓冲区
    std::vector<char> nextBuffer_;           // 备用缓冲区（用于快速交换）
    std::vector<std::vector<char>> buffers_; // 待写入文件的缓冲区队列
    std::ofstream file_;

    static const size_t BUFFER_SIZE = 4 * 1024 * 1024; // 4MB
};

// 日志宏
#define LOG_DEBUG(msg) AsyncLog::instance().write(LogLevel::DEBUG, msg)
#define LOG_INFO(msg) AsyncLog::instance().write(LogLevel::INFO, msg)
#define LOG_WARN(msg) AsyncLog::instance().write(LogLevel::WARN, msg)
#define LOG_ERROR(msg) AsyncLog::instance().write(LogLevel::ERROR, msg)