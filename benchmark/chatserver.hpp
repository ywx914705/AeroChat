#ifndef CHATSERVER_HPP
#define CHATSERVER_HPP
 
#include "EventLoop.hpp"
#include "EventLoopThreadPool.hpp"
#include "User.hpp"
#include "Sock.hpp"
#include "Channel.hpp"
#include <memory>
#include <vector>
#include <atomic>
#include <string>
#include <unordered_map>
/*
AeroChat纯网络层  ,这里为了测网络层性能,所以这里实现一个echo也就是一个回显服务器
补充:
调用listen()与accept()后如果成功都会返回一个fd,这两个fd是不一样的,listen所返回的fd可读意味着有新连接到来
而accept()所返回的fd是分给用户的fd,每一个连接都会有一个fd,当这个fd可读的时候意味着用户发送了数据,服务器
此时可以调用read()读取数据,而当这个fd可写的时候,意味着服务器的发送缓冲区有空闲空间可以调用write()发送数据
(给客户)。

*/
 class ChatServer {
 public:
	 ChatServer(EventLoop* loop, uint16_t port, int numSubReactors);
	 ~ChatServer();
 
	 void start();//开启服务器
	 void handleAccept();//
	 void handleNewConn(int connFd, EventLoop* subLoop);
 
	 static std::atomic<int> connCount_;
 
 private:
	 EventLoop* loop_;//主Loop/主Reactor
	 uint16_t port_;//端口号
	 std::unique_ptr<EventLoopThreadPool> threadPool_;
	 int listenFd_;//listen所对应的fd 注意与accept所返回的fd进行区分
	 std::unique_ptr<Channel> acceptChannel_;//监听fd对应的Channel
	 Sock sock_;//Sock类的实例
 
	 struct PerLoopUsers {
		 std::unordered_map<int, std::shared_ptr<User>> map;
	 };
	 std::vector<std::unique_ptr<PerLoopUsers>> perLoopUsers_;
 
	 static const int MAX_CONNECTIONS = 30000;
 };
 
#endif
