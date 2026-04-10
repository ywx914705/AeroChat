#include "ConnectionPool.hpp"
#include "Log.hpp"
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <chrono>

ConnectionPool& ConnectionPool::getInstance() {
    static ConnectionPool instance;
    return instance;
}

bool ConnectionPool::init(const std::string& host, const std::string& user,
                          const std::string& passwd, const std::string& db,
                          unsigned int port, int poolSize) {
    if (poolSize <= 0) {
        LOG_ERROR("[ConnectionPool] 连接池大小必须大于0");
        return false;
    }

    host_ = host;
    user_ = user;
    passwd_ = passwd;
    db_ = db;
    port_ = port;
    poolSize_ = poolSize;
    stopped_ = false;

    for (int i = 0; i < poolSize_; ++i) {
        MYSQL* conn = createConnection();
        if (conn) {
            connections_.enqueue(conn);
        } else {
            LOG_ERROR("[ConnectionPool] 创建第" + std::to_string(i+1) + "个连接失败");
            MYSQL* tmp;
            while (connections_.try_dequeue(tmp)) {
                mysql_close(tmp);
            }
            return false;
        }
    }

    LOG_INFO("[ConnectionPool] 连接池初始化完成，创建" + std::to_string(poolSize_) + "个连接");
    return true;
}

MYSQL* ConnectionPool::createConnection() {
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) {
        LOG_ERROR("[ConnectionPool] mysql_init失败");
        return nullptr;
    }

    unsigned int timeout = 5;
    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8mb4");

    if (!mysql_real_connect(conn, host_.c_str(), user_.c_str(), passwd_.c_str(),
                            db_.c_str(), port_, nullptr, 0)) {
        LOG_ERROR("[ConnectionPool] mysql_real_connect失败: " + std::string(mysql_error(conn)));
        mysql_close(conn);
        return nullptr;
    }
    return conn;
}

MYSQL* ConnectionPool::ensureValidConnection(MYSQL* conn) {
    if (!conn) {
        LOG_INFO("[ConnectionPool] ensureValidConnection: conn为空，创建新连接");
        return createConnection();
    }
    // 注释掉 mysql_ping，减少网络开销
    // if (mysql_ping(conn) != 0) {
    //     LOG_WARN("[ConnectionPool] 连接失效，重建连接");
    //     mysql_close(conn);
    //     return createConnection();
    // }
    return conn;
}

MYSQL* ConnectionPool::getConnection() {
    MYSQL* conn = nullptr;
    if (connections_.try_dequeue(conn)) {
        conn = ensureValidConnection(conn);
        if (conn) return conn;
    }

    std::unique_lock<std::mutex> lock(cv_mutex_);
    if (!cv_.wait_for(lock, std::chrono::seconds(5), [this, &conn] {
        return connections_.try_dequeue(conn) || stopped_;
    })) {
        LOG_ERROR("[ConnectionPool] 获取连接超时");
        return nullptr;
    }
    if (stopped_) return nullptr;
    conn = ensureValidConnection(conn);
    return conn;
}

void ConnectionPool::releaseConnection(MYSQL* conn) {
    if (!conn || stopped_) {
        if (conn) {
            LOG_INFO("[ConnectionPool] releaseConnection: 连接无效或池已停止，关闭连接");
            mysql_close(conn);
        }
        return;
    }

    conn = ensureValidConnection(conn);
    if (conn) {
        connections_.enqueue(conn);
        cv_.notify_one();
    } else {
        LOG_ERROR("[ConnectionPool] 归还无效连接，已关闭");
        mysql_close(conn);
    }
}

void ConnectionPool::close() {
    stopped_ = true;
    cv_.notify_all();

    MYSQL* conn;
    while (connections_.try_dequeue(conn)) {
        mysql_close(conn);
    }
    LOG_INFO("[ConnectionPool] 连接池已关闭");
}