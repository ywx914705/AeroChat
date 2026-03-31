#pragma once
#include <mysql/mysql.h>
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>

class ConnectionPool {
public:
    static ConnectionPool& getInstance();//单例模式

    bool init(const std::string& host, const std::string& user,
              const std::string& passwd, const std::string& db,
              unsigned int port = 3306, int poolSize = 128);

    MYSQL* getConnection();//获取一个连接
    void releaseConnection(MYSQL* conn);//归还连接
    void close();

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

private:
    ConnectionPool() = default;
    ~ConnectionPool() { close(); }

    MYSQL* createConnection();//创建当连接
    MYSQL* ensureValidConnection(MYSQL* conn);//验证并修复连接

    std::string host_;//数据库IP
    std::string user_;//用户名
    std::string passwd_;//密码
    std::string db_;//数据库名
    unsigned int port_;//端口号(默认3306)
    int poolSize_;//连接池大小

    std::queue<MYSQL*> connections_;//空闲连接队列
    std::mutex mutex_;//保护队列的锁
    std::condition_variable cv_;//条件变量,用于等待连接
    bool stopped_ = false;//连接池是否关闭的标志
};