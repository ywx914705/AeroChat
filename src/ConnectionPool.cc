//ConnectionPool是一个同步连接池,它的职责是管理一组MySQL连接,提供获取与归还的接口
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

    std::lock_guard<std::mutex> lock(mutex_);
    for (int i = 0; i < poolSize_; ++i) {
        MYSQL* conn = createConnection();//初始化连接池,循环调用CreateConnection()建立连接,
        if (conn) {                      //放入connections_队列m如果 
            connections_.push(conn);
        } else {
            LOG_ERROR("[ConnectionPool] 创建第" + std::to_string(i+1) + "个连接失败");
            while (!connections_.empty()) {
				//出错的时候清理已经成功创建的连接,避免资源泄露
                mysql_close(connections_.front());
                connections_.pop();
            }
            return false;
        }
    }

    LOG_INFO("[ConnectionPool] 连接池初始化完成，创建" + std::to_string(connections_.size()) + "个连接");
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
                            //调用mysql_real_connect建立TCP连接并登录,成功返回conn
        LOG_ERROR("[ConnectionPool] mysql_real_connect失败: " + std::string(mysql_error(conn)));
        mysql_close(conn);
        return nullptr;
    }

    return conn;
}

//这个函数保证了用户拿到的连接始终可用！！！
MYSQL* ConnectionPool::ensureValidConnection(MYSQL* conn) {
    if (!conn) {   //确保传入的连接是有效的,如果无效则重新创建
        return createConnection();
    }
      //mysql_ping可以检测新连接是否存活,如果断开会尝试自动重连
    if (mysql_ping(conn) != 0) { //返回0说明连接有效,非0说明连接失败,先调用mysql_close()关闭
                                 //再调用createConnection()新建一个连接返回
        LOG_WARN("[ConnectionPool] 连接失效，重建连接: " + std::string(mysql_error(conn)));
        mysql_close(conn);
        return createConnection();
    }

    return conn;
}
//顾名思义就是从连接池中取出一个可用连接,如果当前没有空闲连接,调用线程会阻塞等待,知道有连接归还
//或者超时
MYSQL* ConnectionPool::getConnection() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (!cv_.wait_for(lock, std::chrono::seconds(5), [this]() { return !connections_.empty() || stopped_; })) {
        LOG_ERROR("[ConnectionPool] 获取连接超时");
        return nullptr;
    }

    if (stopped_) {
        LOG_ERROR("[ConnectionPool] 连接池已停止");
        return nullptr;
    }
	 //如果队列非空或者连接池已停止,立刻返回true并继续  否则线程阻塞,最多等待5秒,期间如果其他线程调用releaseConnection()
	 //归还连接就唤醒该线程

    MYSQL* conn = connections_.front();
    connections_.pop();
    conn = ensureValidConnection(conn);

    int retry = 3;
    while (!conn && retry-- > 0) {
        if (connections_.empty()) {
            if (!cv_.wait_for(lock, std::chrono::seconds(2), [this]() { return !connections_.empty(); })) {
                break;
            }
        }
        if (!connections_.empty()) {
            conn = connections_.front();
            connections_.pop();
            conn = ensureValidConnection(conn);
        }
    }

    if (!conn) {
        LOG_ERROR("[ConnectionPool] 重试3次仍无法获取有效连接");
    }

    return conn;
}

void ConnectionPool::releaseConnection(MYSQL* conn) {
    if (!conn || stopped_) {
        if (conn) mysql_close(conn);
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    conn = ensureValidConnection(conn);
    if (conn) { //如果连接有效就放入队列并调用cv_notify_one()唤醒一个等待获取连接的线程
        connections_.push(conn);
        cv_.notify_one();
    } else {
        LOG_ERROR("[ConnectionPool] 归还无效连接，已关闭");//否则直接关闭,但不归还(因为无效连接不可用！！！)
        mysql_close(conn);
    }
}

void ConnectionPool::close() {  //关闭所有的连接
    std::lock_guard<std::mutex> lock(mutex_);
    stopped_ = true;
    cv_.notify_all();//唤醒所有正在等待getConnection的线程,让它们意识到连接池已经关闭了

    while (!connections_.empty()) {
        MYSQL* conn = connections_.front();//循环清空队列,并对每个连接mysql_close();
        connections_.pop();
        mysql_close(conn);
    }

    LOG_INFO("[ConnectionPool] 连接池已关闭");
}