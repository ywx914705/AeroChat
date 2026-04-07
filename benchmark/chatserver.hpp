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
	 EventLoop* loop_;
	 uint16_t port_;
	 std::unique_ptr<EventLoopThreadPool> threadPool_;
	 int listenFd_;
	 std::unique_ptr<Channel> acceptChannel_;
	 Sock sock_;
 
	 struct PerLoopUsers {
		 std::unordered_map<int, std::shared_ptr<User>> map;
	 };
	 std::vector<std::unique_ptr<PerLoopUsers>> perLoopUsers_;
 
	 static const int MAX_CONNECTIONS = 30000;
 };
 
#endif
