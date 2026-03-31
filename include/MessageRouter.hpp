/*
MessageRouter是AeroChat的业务核心,它负责解析客户端发来的JSON消息,根据消息类型分发到对应的处理函数,并协调
SessionManager、GroupManager、MessageStore等模块完成具体业务。同时,它将耗时操作(如数据库查询)投递到AeroQueue
后台线程池,避免阻塞Reactor线程。
*/
#ifndef MESSAGEROUTER_HPP
#define MESSAGEROUTER_HPP

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

    void doLogin(int fd, std::shared_ptr<User> user, const std::string& account, const std::string& password);
    void doGroupMessage(int fd, std::shared_ptr<User> user, int groupId, const std::string& content);
    void doSingleMessage(int fd, std::shared_ptr<User> user, const std::string& targetUsername, const std::string& content);
    void doPullOffline(int fd, std::shared_ptr<User> user);
    void doHeartbeat(int fd, std::shared_ptr<User> user);
    void doGetOnlineUsers(int fd, std::shared_ptr<User> user);
    void doLoadHistory(int fd, std::shared_ptr<User> user, const std::string& targetType,
                       const std::string& targetId, int limit);
    void doUpdateAvatar(int fd, std::shared_ptr<User> user, const std::string& avatarUrl);
    void doUpdateUsername(int fd, std::shared_ptr<User> user, const std::string& newUsername);
    void doRegister(int fd, const std::string& account, const std::string& password, const std::string& username);
    void doGetConversations(int fd, std::shared_ptr<User> user);

    // 新增
    void doSearchUsers(int fd, std::shared_ptr<User> user, const std::string& keyword, int limit);
    void doGetUserProfile(int fd, std::shared_ptr<User> user, const std::string& targetAccount);

    SessionManager& session_;
    GroupManager& group_;
    MessageStore& store_;
};

#endif // MESSAGEROUTER_HPP