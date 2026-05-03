#include "MessageRouter.hpp"
#include "AeroQueue.hpp"
#include "ConnectionManager.hpp"
#include "DBManager.hpp"
#include "EventLoop.hpp"
#include "Log.hpp"
#include "RedisClient.hpp"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <algorithm>
#include <iostream>
#include <unordered_map>
#include <vector>

MessageRouter &MessageRouter::instance() {
    static MessageRouter instance;
    return instance;
}

MessageRouter::MessageRouter()
    : session_(SessionManager::instance()), group_(GroupManager::instance()),
      store_(MessageStore::instance()) {}

void MessageRouter::onMessage(int fd, const std::string &rawMsg,
                              std::shared_ptr<User> user) {
    std::string msg = rawMsg;
    msg.erase(0, msg.find_first_not_of(" \t\n\r"));
    msg.erase(msg.find_last_not_of(" \t\n\r") + 1);
    if (msg.empty()) return;

    rapidjson::Document doc;
    doc.Parse(msg.c_str());
    if (doc.HasParseError() || !doc.IsObject()) {
        LOG_ERROR("[onMessage] JSON parse error from fd " + std::to_string(fd));
        sendErrorResponse(fd, "Invalid JSON format");
        return;
    }
    if (!doc.HasMember("type") || !doc["type"].IsString()) {
        LOG_ERROR("[onMessage] missing type field from fd " + std::to_string(fd));
        sendErrorResponse(fd, "Missing type field");
        return;
    }

    std::string type = doc["type"].GetString();

    if (type == "login") {
        if (!doc.HasMember("account") || !doc.HasMember("password") ||
            !doc["account"].IsString() || !doc["password"].IsString()) {
            sendErrorResponse(fd, "Missing account or password");
            return;
        }
        std::string account = doc["account"].GetString();
        std::string password = doc["password"].GetString();
		//将登录操作作为任务投递到 AeroQueue
        AeroQueue::instance().post([this, fd, user, account, password]() {
            doLogin(fd, user, account, password);
        });
    } else if (type == "group_message") {
        if (!doc.HasMember("content") || !doc["content"].IsString()) {
            sendErrorResponse(fd, "Missing content");
            return;
        }
        int groupId = 1;
        if (doc.HasMember("groupId") && doc["groupId"].IsInt()) {
            groupId = doc["groupId"].GetInt();
        } else if (doc.HasMember("groupId") && doc["groupId"].IsString()) {
            try {
                groupId = std::stoi(doc["groupId"].GetString());
            } catch (...) {
                sendErrorResponse(fd, "Invalid groupId");
                return;
            }
        } else {
            sendErrorResponse(fd, "Missing groupId");
            return;
        }
        if (groupId < 1 || groupId > 8) {
            sendErrorResponse(fd, "Invalid group ID (must be 1-8)");
            return;
        }
        std::string content = doc["content"].GetString();
        AeroQueue::instance().post([this, fd, user, groupId, content]() {
            doGroupMessage(fd, user, groupId, content);
        });
    } else if (type == "single_message") {
        if (!doc.HasMember("target") || !doc.HasMember("content") ||
            !doc["target"].IsString() || !doc["content"].IsString()) {
            sendErrorResponse(fd, "Missing target or content");
            return;
        }
        std::string target = doc["target"].GetString();
        std::string content = doc["content"].GetString();
        AeroQueue::instance().post([this, fd, user, target, content]() {
            doSingleMessage(fd, user, target, content);
        });
    } else if (type == "pull_offline") {
        AeroQueue::instance().post([this, fd, user]() { doPullOffline(fd, user); });
    } else if (type == "load_history") {
        if (!doc.HasMember("targetType") || !doc.HasMember("targetId") ||
            !doc["targetType"].IsString() || !doc["targetId"].IsString()) {
            sendErrorResponse(fd, "Missing targetType or targetId (must be strings)");
            return;
        }
        std::string targetType = doc["targetType"].GetString();
        std::string targetId = doc["targetId"].GetString();
        int limit = 50;
        int offset = 0;
        if (doc.HasMember("limit") && doc["limit"].IsInt()) limit = doc["limit"].GetInt();
        if (doc.HasMember("offset") && doc["offset"].IsInt()) offset = doc["offset"].GetInt();
        AeroQueue::instance().post([this, fd, user, targetType, targetId, limit, offset]() {
            doLoadHistoryPaginated(fd, user, targetType, targetId, limit, offset);
        });
    } else if (type == "update_avatar") {
        if (!doc.HasMember("avatarUrl") || !doc["avatarUrl"].IsString()) {
            sendErrorResponse(fd, "Missing avatarUrl");
            return;
        }
        std::string avatarUrl = doc["avatarUrl"].GetString();
        AeroQueue::instance().post([this, fd, user, avatarUrl]() {
            doUpdateAvatar(fd, user, avatarUrl);
        });
    } else if (type == "update_username") {
        if (!doc.HasMember("username") || !doc["username"].IsString()) {
            sendErrorResponse(fd, "Missing username");
            return;
        }
        std::string newUsername = doc["username"].GetString();
        AeroQueue::instance().post([this, fd, user, newUsername]() {
            doUpdateUsername(fd, user, newUsername);
        });
    } else if (type == "register") {
        if (!doc.HasMember("account") || !doc.HasMember("password") ||
            !doc.HasMember("username") || !doc["account"].IsString() ||
            !doc["password"].IsString() || !doc["username"].IsString()) {
            sendErrorResponse(fd, "Missing account/password/username");
            return;
        }
        std::string account = doc["account"].GetString();
        std::string password = doc["password"].GetString();
        std::string username = doc["username"].GetString();
        AeroQueue::instance().post([this, fd, account, password, username]() {
            doRegister(fd, account, password, username);
        });
    } else if (type == "ping") {
        doHeartbeat(fd, user);
    } else if (type == "join_group") {
        if (!doc.HasMember("groupId") || !doc["groupId"].IsInt()) {
            sendErrorResponse(fd, "Missing groupId");
            return;
        }
        int groupId = doc["groupId"].GetInt();
        AeroQueue::instance().post([this, fd, user, groupId]() {
            doJoinGroup(fd, user, groupId);
        });
    } else if (type == "leave_group") {
        if (!doc.HasMember("groupId") || !doc["groupId"].IsInt()) {
            sendErrorResponse(fd, "Missing groupId");
            return;
        }
        int groupId = doc["groupId"].GetInt();
        AeroQueue::instance().post([this, fd, user, groupId]() {
            doLeaveGroup(fd, user, groupId);
        });
    } else if (type == "get_groups") {
        AeroQueue::instance().post([this, fd]() { doGetGroups(fd); });
    } else if (type == "get_conversations") {
        AeroQueue::instance().post([this, fd, user]() { doGetConversations(fd, user); });
    } else if (type == "get_group_members") {
        if (!doc.HasMember("groupId") || !doc["groupId"].IsInt()) {
            sendErrorResponse(fd, "Missing groupId");
            return;
        }
        int groupId = doc["groupId"].GetInt();
        int offset = 0;
        int limit = 200;
        if (doc.HasMember("offset") && doc["offset"].IsInt()) offset = doc["offset"].GetInt();
        if (doc.HasMember("limit") && doc["limit"].IsInt()) limit = doc["limit"].GetInt();
        AeroQueue::instance().post([this, fd, groupId, offset, limit]() {
            doGetGroupMembersPaginated(fd, groupId, offset, limit);
        });
    } else if (type == "get_online_users") {
        int page = 1;
        int size = 200;
        if (doc.HasMember("page") && doc["page"].IsInt()) page = doc["page"].GetInt();
        if (doc.HasMember("size") && doc["size"].IsInt()) size = doc["size"].GetInt();
        if (page < 1) page = 1;
        if (size < 1) size = 1;
        if (size > 500) size = 500;
        AeroQueue::instance().post([this, fd, page, size]() {
            doGetOnlineUsersPaginated(fd, page, size);
        });
    } else if (type == "search_users") {
        if (!doc.HasMember("keyword") || !doc["keyword"].IsString()) {
            sendErrorResponse(fd, "Missing keyword");
            return;
        }
        std::string keyword = doc["keyword"].GetString();
        int limit = 20;
        if (doc.HasMember("limit") && doc["limit"].IsInt()) {
            limit = doc["limit"].GetInt();
            if (limit > 100) limit = 100;
        }
        AeroQueue::instance().post([this, fd, user, keyword, limit]() {
            doSearchUsers(fd, user, keyword, limit);
        });
    } else if (type == "get_user_profile") {
        if (!doc.HasMember("account") || !doc["account"].IsString()) {
            sendErrorResponse(fd, "Missing account");
            return;
        }
        std::string targetAccount = doc["account"].GetString();
        AeroQueue::instance().post([this, fd, user, targetAccount]() {
            doGetUserProfile(fd, user, targetAccount);
        });
    } else {
        sendErrorResponse(fd, "Unknown message type");
    }
}

void MessageRouter::sendErrorResponse(int fd, const std::string &errorMsg) {
    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "error", alloc);
    resp.AddMember("message", rapidjson::StringRef(errorMsg.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

// 登录 
void MessageRouter::doLogin(int fd, std::shared_ptr<User> user,
                            const std::string &account,
                            const std::string &password) {
    DBManager &db = DBManager::getInstance();
    int userId = 0;
    std::string username;
    std::string avatarUrl;
    bool success = db.queryAccount(account, password, userId, username, avatarUrl);
    if (!success) {
        sendErrorResponse(fd, "Invalid account or password");
        return;
    }

    if (avatarUrl.empty()) {
        avatarUrl = "https://disk.0voice.com/p/default_avatar.png";
    }

    if (!session_.login(fd, account, userId, username, avatarUrl)) {
        sendErrorResponse(fd, "Login failed");
        return;
    }

    user->setUserId(userId);
    user->setLoggedIn(true);

    auto userGroups = group_.getUserGroups(userId);
    store_.asyncRebuildConversations(userId, false);
    int onlineCount = session_.getOnlineCount();

    rapidjson::Document loginResp;
    loginResp.SetObject();
    auto &alloc = loginResp.GetAllocator();
    loginResp.AddMember("type", "login", alloc);
    loginResp.AddMember("status", "success", alloc);
    loginResp.AddMember("username", rapidjson::StringRef(username.c_str()), alloc);
    loginResp.AddMember("userId", userId, alloc);
    loginResp.AddMember("avatar_url", rapidjson::StringRef(avatarUrl.c_str()), alloc);
    loginResp.AddMember("online_count", onlineCount, alloc);

    rapidjson::Value joinedArray(rapidjson::kArrayType);
    for (int gid : userGroups) {
        joinedArray.PushBack(gid, alloc);
    }
    loginResp.AddMember("joinedGroups", joinedArray, alloc);

    rapidjson::StringBuffer loginBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> loginWriter(loginBuffer);
    loginResp.Accept(loginWriter);
    std::string loginMsg = std::string(loginBuffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, loginMsg);
}

// 群聊消息
void MessageRouter::doGroupMessage(int fd, std::shared_ptr<User> user,
                                   int groupId, const std::string &content) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    int userId = user->getUserId();
    std::string senderAccount = session_.getAccountByFd(fd);
    std::string senderUsername = session_.getUsernameByAccount(senderAccount);
    std::string senderAvatar = session_.getAvatar(senderAccount, "");

    int msgId = store_.saveMessage(userId, senderUsername, senderAvatar, groupId,
                                   true, content);
    if (msgId <= 0) {
        sendErrorResponse(fd, "Failed to save message");
        return;
    }

    // 更新频道最后消息缓存到 Redis
    {
        std::string cacheKey = "group:last_msg:" + std::to_string(groupId);
        time_t now = time(nullptr);
        RedisClient::instance().hset(cacheKey, "content", content.size() > 200 ? content.substr(0, 200) : content);
        RedisClient::instance().hset(cacheKey, "time", std::to_string(static_cast<long long>(now)));
    }

    auto members = group_.getGroupMembers(groupId);
    std::vector<int> onlineMembers;
    std::vector<int> offlineMembers;
    for (int uid : members) {
        std::string account = session_.getAccountByUserId(uid);
        if (account.empty()) account = "test" + std::to_string(uid);
        if (session_.isOnline(account)) {
            onlineMembers.push_back(uid);
        } else {
            offlineMembers.push_back(uid);
        }
    }

    LOG_INFO("[doGroupMessage] groupId=" + std::to_string(groupId) +
             " total=" + std::to_string(members.size()) +
             " online=" + std::to_string(onlineMembers.size()) +
             " offline=" + std::to_string(offlineMembers.size()));

    if (!offlineMembers.empty()) {
        store_.pushToInbox(msgId, offlineMembers);
    }

    rapidjson::Document msgDoc;
    msgDoc.SetObject();
    auto &alloc = msgDoc.GetAllocator();
    msgDoc.AddMember("type", "group_message", alloc);
    msgDoc.AddMember("from", userId, alloc);
    msgDoc.AddMember("from_username", rapidjson::StringRef(senderUsername.c_str()), alloc);
    msgDoc.AddMember("from_avatar", rapidjson::StringRef(senderAvatar.c_str()), alloc);
    msgDoc.AddMember("groupId", groupId, alloc);
    msgDoc.AddMember("content", rapidjson::StringRef(content.c_str()), alloc);
    msgDoc.AddMember("msgId", msgId, alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    msgDoc.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";

    if (onlineMembers.empty()) return;

    std::vector<std::string> onlineAccounts;
    onlineAccounts.reserve(onlineMembers.size());
    for (int uid : onlineMembers) {
        std::string account = session_.getAccountByUserId(uid);
        if (account.empty()) account = "test" + std::to_string(uid);
        onlineAccounts.push_back(account);
    }

    std::vector<int> fds = session_.getFdsByAccounts(onlineAccounts);
    auto users = ConnectionManager::instance().getUsers(fds);

    std::unordered_map<EventLoop *, std::vector<std::shared_ptr<User>>> threadUsers;
    for (size_t i = 0; i < fds.size(); ++i) {
        if (users[i] && fds[i] != -1) {
            threadUsers[users[i]->getLoop()].push_back(users[i]);
        }
    }

    for (auto &[loop, userList] : threadUsers) {
        loop->runInLoop([userList, msg]() {
            for (auto &targetUser : userList) {
                targetUser->send(msg.data(), msg.size());
            }
        });
    }
}

// 私聊消息（优化版：使用账号）
void MessageRouter::doSingleMessage(int fd, std::shared_ptr<User> user,
                                    const std::string &target,
                                    const std::string &content) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }

    int fromUserId = user->getUserId();
    std::string senderAccount = session_.getAccountByFd(fd);
    std::string senderUsername = session_.getUsernameByAccount(senderAccount);
    std::string senderAvatar = session_.getAvatar(senderAccount, "");

    std::string targetAccount = session_.getAccountByAccountOrUsername(target);
    if (targetAccount.empty()) {
        sendErrorResponse(fd, "Target user not found");
        return;
    }

    int msgId = store_.saveMessage(fromUserId, senderUsername, senderAvatar,
                                   targetAccount, content);
    if (msgId <= 0) {
        sendErrorResponse(fd, "Failed to save message");
        return;
    }

    int toUserId = session_.getUserIdByAccount(targetAccount);
    bool isOnline = session_.isOnline(targetAccount);
    int targetFd = session_.getFdByAccount(targetAccount);

    rapidjson::Document msgDoc;
    msgDoc.SetObject();
    auto &alloc = msgDoc.GetAllocator();
    msgDoc.AddMember("type", "single_message", alloc);
    msgDoc.AddMember("from", fromUserId, alloc);
    msgDoc.AddMember("from_username", rapidjson::StringRef(senderUsername.c_str()), alloc);
    msgDoc.AddMember("from_avatar", rapidjson::StringRef(senderAvatar.c_str()), alloc);
    msgDoc.AddMember("content", rapidjson::StringRef(content.c_str()), alloc);
    msgDoc.AddMember("msgId", msgId, alloc);
    msgDoc.AddMember("to", rapidjson::StringRef(target.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    msgDoc.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";

    if (isOnline && targetFd != -1) {
        ConnectionManager::instance().sendToUser(targetFd, msg);
    } else if (toUserId > 0) {
        store_.pushToInbox(msgId, {toUserId});
    }

    rapidjson::Document receipt;
    receipt.SetObject();
    receipt.AddMember("type", "single_message_ack", receipt.GetAllocator());
    receipt.AddMember("status", "sent", receipt.GetAllocator());
    rapidjson::StringBuffer receiptBuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer2(receiptBuf);
    receipt.Accept(writer2);
    std::string receiptMsg = std::string(receiptBuf.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, receiptMsg);
}

// 离线消息 
void MessageRouter::doPullOffline(int fd, std::shared_ptr<User> user) {
    if (!user->isLoggedIn()) return;
    int userId = user->getUserId();
    auto ids = store_.pullOfflineMsgIds(userId);
    auto msgs = store_.getMessages(ids);

    for (auto &msg : msgs) {
        std::string account = session_.getAccountByUserId(msg.fromUserId);
        if (account.empty()) account = "test" + std::to_string(msg.fromUserId);
        std::string currentAvatar = session_.getAvatar(account, msg.fromAvatar);
        if (!currentAvatar.empty()) msg.fromAvatar = currentAvatar;
        std::string currentUsername = session_.getUsernameByAccount(account);
        if (!currentUsername.empty()) msg.fromUsername = currentUsername;
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "offline_messages", alloc);
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto &m : msgs) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("from", m.fromUserId, alloc);
        obj.AddMember("from_username", rapidjson::StringRef(m.fromUsername.c_str()), alloc);
        obj.AddMember("from_avatar", rapidjson::StringRef(m.fromAvatar.c_str()), alloc);
        obj.AddMember("type", m.type, alloc);
        obj.AddMember("content", rapidjson::StringRef(m.content.c_str()), alloc);
        obj.AddMember("msgId", m.msgId, alloc);
        if (m.type == 0) obj.AddMember("groupId", m.toId, alloc);
        arr.PushBack(obj, alloc);
    }
    resp.AddMember("messages", arr, alloc);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

//心跳
void MessageRouter::doHeartbeat(int fd, std::shared_ptr<User> user) {
    (void)user;
    ConnectionManager::instance().sendToUser(fd, "{\"type\":\"pong\"}\n");
}

// 分页获取在线用户（使用 Sorted Set 服务端分页，不再全量加载）
void MessageRouter::doGetOnlineUsersPaginated(int fd, int page, int size) {
    int total = session_.getOnlineCount();
    int start = (page - 1) * size;

    // 使用 Sorted Set 服务端分页，只获取当前页数据
    auto pageUsers = session_.getOnlineUsersPaginated(start, size);

    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "online_users", alloc);
    resp.AddMember("page", page, alloc);
    resp.AddMember("size", size, alloc);
    resp.AddMember("total", total, alloc);

    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto &info : pageUsers) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("account", rapidjson::StringRef(info.account.c_str()), alloc);
        obj.AddMember("username", rapidjson::StringRef(info.username.c_str()), alloc);
        obj.AddMember("avatar_url", rapidjson::StringRef(info.avatarUrl.c_str()), alloc);
        arr.PushBack(obj, alloc);
    }
    resp.AddMember("users", arr, alloc);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

// 加载历史消息（分页）
void MessageRouter::doLoadHistoryPaginated(int fd, std::shared_ptr<User> user,
                                           const std::string &targetType,
                                           const std::string &targetId,
                                           int limit, int offset) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }

    LOG_INFO("[doLoadHistory] fd=" + std::to_string(fd) +
             ", targetType=" + targetType + ", targetId=" + targetId +
             ", limit=" + std::to_string(limit) + ", offset=" + std::to_string(offset));

    std::vector<StoredMessage> history;
    if (targetType == "group") {
        int groupId = 0;
        try {
            groupId = std::stoi(targetId);
        } catch (const std::exception &e) {
            LOG_ERROR("[doLoadHistory] Invalid group id: " + targetId);
            sendErrorResponse(fd, "Invalid group id");
            return;
        }
        history = store_.loadHistoryPaginated(groupId, limit, offset);
        LOG_INFO("[doLoadHistory] group history count=" + std::to_string(history.size()) +
                 " for groupId=" + std::to_string(groupId));
    } else if (targetType == "private") {
        std::string targetAccount = session_.getAccountByAccountOrUsername(targetId);
        if (targetAccount.empty()) {
            LOG_ERROR("[doLoadHistory] Target user not found: " + targetId);
            sendErrorResponse(fd, "Target user not found");
            return;
        }
        std::string myAccount = session_.getAccountByFd(fd);
        if (myAccount.empty()) {
            sendErrorResponse(fd, "Invalid user");
            return;
        }
        history = store_.loadHistoryPaginated(myAccount, targetAccount, limit, offset);
        LOG_INFO("[doLoadHistory] private history count=" + std::to_string(history.size()) +
                 " between " + myAccount + " and " + targetAccount);
    } else {
        LOG_ERROR("[doLoadHistory] Invalid targetType: " + targetType);
        sendErrorResponse(fd, "Invalid target type");
        return;
    }

    for (auto &msg : history) {
        std::string account = session_.getAccountByUserId(msg.fromUserId);
        if (account.empty()) account = "test" + std::to_string(msg.fromUserId);
        std::string currentAvatar = session_.getAvatar(account, msg.fromAvatar);
        if (!currentAvatar.empty()) msg.fromAvatar = currentAvatar;
        std::string currentUsername = session_.getUsernameByAccount(account);
        if (!currentUsername.empty()) msg.fromUsername = currentUsername;
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "load_history", alloc);
    resp.AddMember("targetType", rapidjson::StringRef(targetType.c_str()), alloc);
    resp.AddMember("targetId", rapidjson::StringRef(targetId.c_str()), alloc);
    resp.AddMember("offset", offset, alloc);
    resp.AddMember("limit", limit, alloc);

    rapidjson::Value msgArray(rapidjson::kArrayType);
    for (const auto &msg : history) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("msgId", msg.msgId, alloc);
        obj.AddMember("from", msg.fromUserId, alloc);
        obj.AddMember("from_username", rapidjson::StringRef(msg.fromUsername.c_str()), alloc);
        obj.AddMember("from_avatar", rapidjson::StringRef(msg.fromAvatar.c_str()), alloc);
        obj.AddMember("content", rapidjson::StringRef(msg.content.c_str()), alloc);
        obj.AddMember("sendTime", static_cast<int>(msg.sendTime), alloc);
        if (msg.type == 0) obj.AddMember("groupId", msg.toId, alloc);
        msgArray.PushBack(obj, alloc);
    }
    resp.AddMember("messages", msgArray, alloc);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

// 更新头像
void MessageRouter::doUpdateAvatar(int fd, std::shared_ptr<User> user,
                                   const std::string &avatarUrl) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }

    std::string account = session_.getAccountByFd(fd);
    if (account.empty()) {
        sendErrorResponse(fd, "User not found");
        return;
    }

    bool db_ok = DBManager::getInstance().updateUserAvatar(account, avatarUrl);
    if (!db_ok) {
        sendErrorResponse(fd, "Failed to update avatar");
        return;
    }

    session_.setAvatar(account, avatarUrl);

    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "update_avatar", alloc);
    resp.AddMember("status", "success", alloc);
    resp.AddMember("avatar_url", rapidjson::StringRef(avatarUrl.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

// 更新用户名 
void MessageRouter::doUpdateUsername(int fd, std::shared_ptr<User> user,
                                     const std::string &newUsername) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }

    if (!session_.updateUsername(fd, newUsername)) {
        sendErrorResponse(fd, "Failed to update username");
        return;
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "update_username", alloc);
    resp.AddMember("status", "success", alloc);
    resp.AddMember("username", rapidjson::StringRef(newUsername.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

//注册
void MessageRouter::doRegister(int fd, const std::string &account,
                               const std::string &password,
                               const std::string &username) {
   /* std::string existingUsername;
    if (DBManager::getInstance().queryUsernameByAccount(account, existingUsername)) {
        sendErrorResponse(fd, "Account already exists");
        return;
    }
    */
    if (DBManager::getInstance().isAccountExist(account)) {
        sendErrorResponse(fd, "Account already exists");
        return;
    }

    std::string hashedPassword = DBManager::sha256(password);

    int userId = 0;
    bool ok = DBManager::getInstance().insertUser(account, hashedPassword, username, userId);
    if (!ok || userId <= 0) {
        sendErrorResponse(fd, "Failed to register, database error");
        return;
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "register", alloc);
    resp.AddMember("status", "success", alloc);
    resp.AddMember("account", rapidjson::StringRef(account.c_str()), alloc);
    resp.AddMember("username", rapidjson::StringRef(username.c_str()), alloc);
    resp.AddMember("avatar_url", rapidjson::StringRef("https://disk.0voice.com/p/default_avatar.png"), alloc);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

//搜索用户
void MessageRouter::doSearchUsers(int fd, std::shared_ptr<User> user,
                                  const std::string &keyword, int limit) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    auto results = session_.searchAllUsers(keyword, limit);
    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "search_users", alloc);
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto &[acc, uname, avatar] : results) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("account", rapidjson::StringRef(acc.c_str()), alloc);
        obj.AddMember("username", rapidjson::StringRef(uname.c_str()), alloc);
        obj.AddMember("avatar_url", rapidjson::StringRef(avatar.c_str()), alloc);
        arr.PushBack(obj, alloc);
    }
    resp.AddMember("users", arr, alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

// 获取用户主页
void MessageRouter::doGetUserProfile(int fd, std::shared_ptr<User> user,
                                     const std::string &targetAccount) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    std::string realAccount = session_.getAccountByAccountOrUsername(targetAccount);
    if (realAccount.empty()) {
        sendErrorResponse(fd, "User not found");
        return;
    }
    auto profile = session_.getUserProfile(realAccount);
    if (profile.account.empty()) {
        sendErrorResponse(fd, "User not found");
        return;
    }
    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "user_profile", alloc);
    resp.AddMember("account", rapidjson::StringRef(profile.account.c_str()), alloc);
    resp.AddMember("username", rapidjson::StringRef(profile.username.c_str()), alloc);
    resp.AddMember("avatar_url", rapidjson::StringRef(profile.avatarUrl.c_str()), alloc);
    resp.AddMember("bio", rapidjson::StringRef(profile.bio.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

// 群组操作
void MessageRouter::doJoinGroup(int fd, std::shared_ptr<User> user, int groupId) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    int userId = user->getUserId();
    bool ok = group_.addUserToGroup(groupId, userId, 3);
    if (!ok) {
        sendErrorResponse(fd, "Join group failed: group full, already joined, or limit reached");
        return;
    }
    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "join_group", alloc);
    resp.AddMember("status", "success", alloc);
    resp.AddMember("groupId", groupId, alloc);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    resp.Accept(writer);
    std::string msg = std::string(buf.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doLeaveGroup(int fd, std::shared_ptr<User> user, int groupId) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    int userId = user->getUserId();
    bool ok = group_.removeUserFromGroup(groupId, userId);
    if (!ok) {
        sendErrorResponse(fd, "Leave group failed");
        return;
    }
    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "leave_group", alloc);
    resp.AddMember("status", "success", alloc);
    resp.AddMember("groupId", groupId, alloc);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    resp.Accept(writer);
    std::string msg = std::string(buf.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doGetGroups(int fd) {
    auto allGroups = group_.getAllGroups();
    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "group_list", alloc);
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto &g : allGroups) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("id", g.id, alloc);
        obj.AddMember("name", rapidjson::StringRef(g.name.c_str()), alloc);
        obj.AddMember("description", rapidjson::StringRef(g.description.c_str()), alloc);
        obj.AddMember("avatar", rapidjson::StringRef(g.avatar.c_str()), alloc);
        obj.AddMember("memberCount", g.memberCount, alloc);
        obj.AddMember("maxMembers", g.maxMembers, alloc);

        // 从 Redis 缓存读取频道最后消息（替代 MySQL 查询）
        std::string cacheKey = "group:last_msg:" + std::to_string(g.id);
        std::string lastMsg = RedisClient::instance().hget(cacheKey, "content");
        std::string lastTimeStr = RedisClient::instance().hget(cacheKey, "time");
        int lastTime = 0;
        if (!lastTimeStr.empty()) {
            try { lastTime = std::stoi(lastTimeStr); } catch (...) {}
        }
        obj.AddMember("lastMsg", rapidjson::StringRef(lastMsg.c_str()), alloc);
        obj.AddMember("lastTime", lastTime, alloc);

        arr.PushBack(obj, alloc);
    }
    resp.AddMember("groups", arr, alloc);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    resp.Accept(writer);
    std::string msg = std::string(buf.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

// 获取私聊会话列表 
void MessageRouter::doGetConversations(int fd, std::shared_ptr<User> user) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    int userId = user->getUserId();
    std::string convKey = "user:conv:" + std::to_string(userId);

    auto allFields = RedisClient::instance().hgetall(convKey);
    std::unordered_map<std::string, std::tuple<std::string, std::string, int>> convData;

    for (const auto &[field, value] : allFields) {
        size_t colon = field.find(':');
        if (colon == std::string::npos) continue;
        std::string prefix = field.substr(0, colon);
        std::string suffix = field.substr(colon + 1);
        if (suffix == "lastMsg") {
            convData[prefix] = std::make_tuple(value, "", 0);
        } else if (suffix == "lastTime") {
            auto &data = convData[prefix];
            std::get<1>(data) = value;
        } else if (suffix == "unread") {
            auto &data = convData[prefix];
            try {
                std::get<2>(data) = std::stoi(value);
            } catch (...) {}
        }
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "conversations", alloc);
    rapidjson::Value convsArray(rapidjson::kArrayType);

    for (const auto &[prefix, data] : convData) {
        if (prefix.find("single:") != 0) continue;
        std::string peerAccount = prefix.substr(7);
        std::string username = session_.getUsernameByAccount(peerAccount);
        std::string avatar = session_.getAvatar(peerAccount, "");
        if (username.empty()) username = peerAccount;

        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("type", "single", alloc);
        obj.AddMember("id", rapidjson::StringRef(peerAccount.c_str()), alloc);
        obj.AddMember("lastMsg", rapidjson::StringRef(std::get<0>(data).c_str()), alloc);
        int lastTime = 0;
        if (!std::get<1>(data).empty()) lastTime = std::stoi(std::get<1>(data));
        obj.AddMember("lastTime", lastTime, alloc);
        obj.AddMember("unread", std::get<2>(data), alloc);
        obj.AddMember("name", rapidjson::StringRef(username.c_str()), alloc);
        obj.AddMember("avatar", rapidjson::StringRef(avatar.c_str()), alloc);
        convsArray.PushBack(obj, alloc);
    }

    resp.AddMember("conversations", convsArray, alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

//分页获取群组成员 
void MessageRouter::doGetGroupMembersPaginated(int fd, int groupId, int offset, int limit) {
    auto members = group_.getGroupMembersPaginated(groupId, offset, limit);
    rapidjson::Document resp;
    resp.SetObject();
    auto &alloc = resp.GetAllocator();
    resp.AddMember("type", "group_members", alloc);
    resp.AddMember("groupId", groupId, alloc);
    resp.AddMember("offset", offset, alloc);
    resp.AddMember("limit", limit, alloc);
    rapidjson::Value arr(rapidjson::kArrayType);
    for (int uid : members) {
        std::string account = session_.getAccountByUserId(uid);
        if (account.empty()) account = "test" + std::to_string(uid);
        std::string username = session_.getUsernameByAccount(account);
        std::string avatar = session_.getAvatar(account, "");
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("userId", uid, alloc);
        obj.AddMember("account", rapidjson::StringRef(account.c_str()), alloc);
        obj.AddMember("username", rapidjson::StringRef(username.c_str()), alloc);
        obj.AddMember("avatar_url", rapidjson::StringRef(avatar.c_str()), alloc);
        arr.PushBack(obj, alloc);
    }
    resp.AddMember("members", arr, alloc);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    resp.Accept(writer);
    std::string msg = std::string(buf.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}
