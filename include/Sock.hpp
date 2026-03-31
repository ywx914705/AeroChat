#pragma once
#include "noncopyable.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <cerrno>
#include <cstdio>
//对linux中的接口做封装  即socket()、bind()、listen()、accept()
class Sock : noncopyable {
public:
    int Socket() {
        int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            perror("socket create failed");
            exit(1);
        }
        SetReuseAddr(fd, true);
        return fd;
    }

    void SetReuseAddr(int fd, bool on) {//单独设置地址复用选项
        int opt = on ? 1 : 0;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    }

    void SetNonBlock(int fd) {//设置fd为非阻塞,配合epoll的ET模式
        int flags = fcntl(fd, F_GETFL);
        if (flags == -1) {
            perror("fcntl get flags failed");
            return;
        }
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
            perror("fcntl set nonblock failed");
        }
    }

    bool Bind(int fd, uint16_t port) {
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);
        if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            perror("bind failed");
            close(fd);
            return false;
        }
        return true;
    }

    bool Listen(int fd, int backlog = 10240) {
        if (listen(fd, backlog) < 0) {
            perror("listen failed");
            close(fd);
            return false;
        }
        return true;
    }

    int Accept(int listenfd, std::string* clientip, uint16_t* clientport) {
        struct sockaddr_in clientaddr;
        socklen_t len = sizeof(clientaddr);
        int connfd = accept4(listenfd, (struct sockaddr*)&clientaddr, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (connfd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                perror("accept failed");
            }
            return -1;
        }
        *clientip = inet_ntoa(clientaddr.sin_addr);
        *clientport = ntohs(clientaddr.sin_port);
        return connfd;
    }
};