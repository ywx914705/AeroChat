#pragma once
#include <mysql/mysql.h>
#include <string>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include "concurrentqueue.hpp"

class ConnectionPool {
public:
    static ConnectionPool& getInstance();//单例模式,全局唯一连接池

    bool init(const std::string& host, const std::string& user,
              const std::string& passwd, const std::string& db,
              unsigned int port = 3306, int poolSize = 128);//初始化连接池

    MYSQL* getConnection();//从连接池中获取一个连接
    void releaseConnection(MYSQL* conn);//归还一个连接
    void close();//关闭
    //禁止拷贝/赋值操作
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

private:
    ConnectionPool() = default;
    ~ConnectionPool() { close(); }

    MYSQL* createConnection();//创建一条mysql连接
    MYSQL* ensureValidConnection(MYSQL* conn);//检验连接是否有效

    std::string host_;
    std::string user_;
    std::string passwd_;
    std::string db_;
    unsigned int port_;
    int poolSize_;

    moodycamel::ConcurrentQueue<MYSQL*> connections_;//存储所有连接的一个无锁队列
    std::atomic<bool> stopped_{false};//是否停止
    std::mutex cv_mutex_;
    std::condition_variable cv_;
};