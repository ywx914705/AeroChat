/*
程序的入口:
*/
#include "chatserver.hpp"
#include "Log.hpp"
#include <iostream>
#include <signal.h>
#include "EventLoop.hpp"
#include "RedisClient.hpp"
#include "AeroQueue.hpp"

int main(int argc, char* argv[]) {
    //1、初始化异步日志
    AsyncLog::instance().init("server.log", 3);
    LOG_INFO("Server starting...");

    signal(SIGPIPE, SIG_IGN);
    if (argc < 2) {
        printf("Usage: %s port\n", argv[0]);
        return 1;
    }

    // 科学设置工作线程数：
    // 对于 I/O 密集型任务（数据库查询、Redis操作），线程数可以大于 CPU 核心数，
    // 因为线程大部分时间在等待 I/O。通常推荐为 CPU 核心数的 2~4 倍。
    // 但必须与数据库连接池大小匹配，否则会造成连接闲置或连接竞争。
    // 当前数据库连接池大小在 ChatServer 中配置为 DB_POOL_SIZE = 1000，
    // 但实际 MySQL 可能无法支撑 1000 个连接，建议将连接池大小调至 150~200，
    // 并将工作线程数也设为相同数值，以达到最佳并发处理能力。
    // 这里我们设置工作线程数为 150，与建议的连接池大小一致。
    unsigned int threadCount = 500;
    // 如果不想硬编码，也可以使用公式：CPU核心数 * 8（经验值，适合 I/O 密集型）
    // unsigned int threadCount = std::thread::hardware_concurrency() * 8;
    // if (threadCount == 0) threadCount = 32;
    AeroQueue::instance().start(threadCount);

    // Redis 连接池保持 500 个（可根据需要调整）
    if (!RedisClient::instance().init("127.0.0.1", 6379, 500)) {
        LOG_ERROR("Redis init failed");
        return 1;
    }

    uint16_t port = static_cast<uint16_t>(atoi(argv[1]));
    EventLoop loop;//主EventLoop
    loop.setIndex(0);
    ChatServer server(&loop, port, 8);
   
    server.start();
    loop.loop();
     
    return 0;
}