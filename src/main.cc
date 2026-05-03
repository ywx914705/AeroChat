/*
程序的入口:
*/
#include "chatserver.hpp"
#include "Log.hpp"
#include <iostream>
#include <thread>
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

    // 工作线程数 = CPU核心数 * 4（I/O密集型推荐）
    unsigned int threadCount = std::thread::hardware_concurrency() * 4;
    if (threadCount == 0) threadCount = 32;
    AeroQueue::instance().start(threadCount);

    // Redis 连接池 64（Redis 单线程，无需过多连接）
    if (!RedisClient::instance().init("127.0.0.1", 6379, 64)) {
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