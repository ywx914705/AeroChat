#ifndef MESSAGEROUTER_HPP
#define MESSAGEROUTER_HPP
/*
MessageRouter是AeroChat的业务核心,它负责解析客户端发来的JSON消息,根据消息类型分发到对应的处理函数,并协调
SessionManager、GroupManager、MessageStore等模块完成具体业务。同时,它将耗时操作(如数据库查询)投递到AeroQueue
后台线程池,避免阻塞Reactor线程。
*/
#include "User.hpp"
#include "SessionManager.hpp"
#include "GroupManager.hpp"
#include "MessageStore.hpp"
#include <memory>
#include <string>

class MessageRouter {
public:
    static MessageRouter& instance();

    void onMessage(int fd, const std::string& rawMsg, std::shared_ptr<User> user);

private:
    MessageRouter();
    void sendErrorResponse(int fd, const std::string& errorMsg);
//认证与用户管理
    void doLogin(int fd, std::shared_ptr<User> user, const std::string& account, const std::string& password);//验证账号密码,以及创建会话,返回用户信息
    
    void doRegister(int fd, const std::string& account, const std::string& password, const std::string& username);//注册新用户,写入数据库
    
	void doUpdateUsername(int fd, std::shared_ptr<User> user, const std::string& newUsername);//修改名称,更新Redis与MySQL
	
    void doUpdateAvatar(int fd, std::shared_ptr<User> user, const std::string& avatarUrl);//更新头像 URL的形式
    
	void doGetUserProfile(int fd, std::shared_ptr<User> user, const std::string& targetAccount);//获取指定用户的公开资料
	

//消息转发
    void doGroupMessage(int fd, std::shared_ptr<User> user, int groupId, const std::string& content);//频道消息
    
    void doSingleMessage(int fd, std::shared_ptr<User> user, const std::string& target, const std::string& content);//私聊消息
	
	 void doLoadHistoryPaginated(int fd, std::shared_ptr<User> user, const std::string& targetType,const std::string& targetId, int limit, int offset);
	 //分页加载历史消息
    void doPullOffline(int fd, std::shared_ptr<User> user);//拉取离线消息
    
    void doHeartbeat(int fd, std::shared_ptr<User> user);//ping pong 测试连通性


// 群组操作   这里的       群组与频道是一个概念！！！
    void doJoinGroup(int fd, std::shared_ptr<User> user, int groupId);//加入群组/频道
    void doLeaveGroup(int fd, std::shared_ptr<User> user, int groupId);//退出频道
    void doGetGroups(int fd);//获取所有群组列表
    void doGetGroupMembersPaginated(int fd, int groupId, int offset, int limit);//分页获取群组成员

//会话与在线用户
    void doGetConversations(int fd, std::shared_ptr<User> user);//获取私聊会话列表
    
    void doGetOnlineUsersPaginated(int fd, int page, int size);//分页获取在线用户列表
    
    void doSearchUsers(int fd, std::shared_ptr<User> user, const std::string& keyword, int limit);//根据关键字搜索用户(支持模糊查询)



    SessionManager& session_;
    GroupManager& group_;
    MessageStore& store_;
};

#endif // MESSAGEROUTER_HPP