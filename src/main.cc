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

    unsigned int threadCount = std::thread::hardware_concurrency() * 2;
    if (threadCount == 0) threadCount = 16;
	//2、开启任务线程
    AeroQueue::instance().start(threadCount);

    // Redis 连接池保持 128 个
    if (!RedisClient::instance().init("127.0.0.1", 6379, 128)) {
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