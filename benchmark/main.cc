#include "chatserver.hpp"
#include "EventLoop.hpp"
#include <signal.h>
#include <iostream>
#include <thread> 

int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);

    if (argc < 2) {
        printf("Usage: %s port\n", argv[0]);
        return 1;
    }

    uint16_t port = static_cast<uint16_t>(atoi(argv[1]));
    EventLoop loop; //创建一个EventLoop对象loop
    int subReactorNum = std::thread::hardware_concurrency();//由于我的服务器是8核8GB,所以这里的
    //subReactorNum=我的cpu核心数也就是等于8
    ChatServer server(&loop, port, subReactorNum);

    server.start();//启动服务器
    loop.loop();//开始loop
	printf("服务器启动成功！\n");

    return 0;
}
