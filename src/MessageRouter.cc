#include "MessageRouter.hpp"
#include "ConnectionManager.hpp"
#include "DBManager.hpp"
#include "Log.hpp"
#include "EventLoop.hpp"
#include "AeroQueue.hpp"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <vector>

struct GroupInfo {
    int id;
    std::string name;
    std::string avatar;
};

static const std::vector<GroupInfo> PRESET_GROUPS = {
    {1, "💻 编程技术交流", "https://cdn-icons-png.flaticon.com/512/1998/1998592.png"},
    {2, "🍵 生活杂谈", "https://cdn-icons-png.flaticon.com/512/3075/3075977.png"},
    {3, "🎮 游戏娱乐", "https://cdn-icons-png.flaticon.com/512/3659/3659788.png"}
};

MessageRouter& MessageRouter::instance() {
    static MessageRouter instance;
    return instance;
}

MessageRouter::MessageRouter()
    : session_(SessionManager::instance())
    , group_(GroupManager::instance())
    , store_(MessageStore::instance()) {}

void MessageRouter::onMessage(int fd, const std::string& rawMsg, std::shared_ptr<User> user) {
    rapidjson::Document doc;
    doc.Parse(rawMsg.c_str());
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
        AeroQueue::instance().post([this, fd, user, account, password]() {
            doLogin(fd, user, account, password);
        });
    }
    else if (type == "group_message") {
        if (!doc.HasMember("content") || !doc["content"].IsString()) {
            sendErrorResponse(fd, "Missing content");
            return;
        }
        int groupId = 1;
        std::string rawGroupId;
        if (doc.HasMember("groupId") && doc["groupId"].IsInt()) {
            groupId = doc["groupId"].GetInt();
            rawGroupId = std::to_string(groupId);
        } else if (doc.HasMember("groupId") && doc["groupId"].IsString()) {
            rawGroupId = doc["groupId"].GetString();
            try {
                groupId = std::stoi(rawGroupId);
            } catch (const std::exception& e) {
                LOG_ERROR("[onMessage] groupId string to int failed: " + rawGroupId);
                sendErrorResponse(fd, "Invalid groupId format");
                return;
            }
        } else {
            sendErrorResponse(fd, "Missing or invalid groupId");
            return;
        }
        if (groupId < 1 || groupId > 3) {
            sendErrorResponse(fd, "Invalid group ID (must be 1-3)");
            return;
        }
        std::string content = doc["content"].GetString();
        AeroQueue::instance().post([this, fd, user, groupId, content]() {
            doGroupMessage(fd, user, groupId, content);
        });
    }
    else if (type == "single_message") {
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
    }
    else if (type == "pull_offline") {
        AeroQueue::instance().post([this, fd, user]() {
            doPullOffline(fd, user);
        });
    }
    else if (type == "load_history") {
        if (!doc.HasMember("targetType") || !doc.HasMember("targetId") ||
            !doc["targetType"].IsString() || !doc["targetId"].IsString()) {
            sendErrorResponse(fd, "Missing targetType or targetId (must be strings)");
            return;
        }
        std::string targetType = doc["targetType"].GetString();
        std::string targetId = doc["targetId"].GetString();
        int limit = 50;
        if (doc.HasMember("limit")) {
            if (doc["limit"].IsInt()) {
                limit = doc["limit"].GetInt();
            } else if (doc["limit"].IsString()) {
                try { limit = std::stoi(doc["limit"].GetString()); } catch (...) {}
            }
        }
        AeroQueue::instance().post([this, fd, user, targetType, targetId, limit]() {
            doLoadHistory(fd, user, targetType, targetId, limit);
        });
    }
    else if (type == "update_avatar") {
        if (!doc.HasMember("avatarUrl") || !doc["avatarUrl"].IsString()) {
            sendErrorResponse(fd, "Missing avatarUrl");
            return;
        }
        std::string avatarUrl = doc["avatarUrl"].GetString();
        AeroQueue::instance().post([this, fd, user, avatarUrl]() {
            doUpdateAvatar(fd, user, avatarUrl);
        });
    }
    else if (type == "update_username") {
        if (!doc.HasMember("username") || !doc["username"].IsString()) {
            sendErrorResponse(fd, "Missing username");
            return;
        }
        std::string newUsername = doc["username"].GetString();
        AeroQueue::instance().post([this, fd, user, newUsername]() {
            doUpdateUsername(fd, user, newUsername);
        });
    }
    else if (type == "register") {
        if (!doc.HasMember("account") || !doc.HasMember("password") || !doc.HasMember("username") ||
            !doc["account"].IsString() || !doc["password"].IsString() || !doc["username"].IsString()) {
            sendErrorResponse(fd, "Missing account/password/username");
            return;
        }
        std::string account = doc["account"].GetString();
        std::string password = doc["password"].GetString();
        std::string username = doc["username"].GetString();
        AeroQueue::instance().post([this, fd, account, password, username]() {
            doRegister(fd, account, password, username);
        });
    }
    else if (type == "ping") {
        doHeartbeat(fd, user);
    }
    else if (type == "get_online_users") {
        // 前端已改用推送，此接口不再需要，但为了兼容返回空列表
        doGetOnlineUsers(fd, user);
    }
    else if (type == "get_conversations") {
        doGetConversations(fd, user);
    }
    else if (type == "search_users") {
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
    }
    else if (type == "get_user_profile") {
        if (!doc.HasMember("account") || !doc["account"].IsString()) {
            sendErrorResponse(fd, "Missing account");
            return;
        }
        std::string targetAccount = doc["account"].GetString();
        AeroQueue::instance().post([this, fd, user, targetAccount]() {
            doGetUserProfile(fd, user, targetAccount);
        });
    }
    else {
        LOG_ERROR("[onMessage] unknown type: " + type + " from fd " + std::to_string(fd));
        sendErrorResponse(fd, "Unknown message type");
    }
}

void MessageRouter::sendErrorResponse(int fd, const std::string& errorMsg) {
    rapidjson::Document resp;
    resp.SetObject();
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "error", alloc);
    resp.AddMember("message", rapidjson::StringRef(errorMsg.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doLogin(int fd, std::shared_ptr<User> user, const std::string& account, const std::string& password) {
    DBManager& db = DBManager::getInstance();
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

    GroupManager& gm = GroupManager::instance();
    for (int gid = 1; gid <= 3; ++gid) {
        gm.addUserToGroup(gid, userId);
    }

    store_.asyncRebuildConversations(userId, false);

    // 获取在线用户列表，只返回前100个
    auto allOnlineUsers = session_.getAllOnlineUserInfos();
    std::vector<decltype(allOnlineUsers)::value_type> onlineUsers;
    for (size_t i = 0; i < allOnlineUsers.size() && i < 100; ++i) {
        onlineUsers.push_back(allOnlineUsers[i]);
    }

    rapidjson::Document loginResp;
    loginResp.SetObject();
    auto& alloc = loginResp.GetAllocator();
    loginResp.AddMember("type", "login", alloc);
    loginResp.AddMember("status", "success", alloc);
    loginResp.AddMember("username", rapidjson::StringRef(username.c_str()), alloc);
    loginResp.AddMember("userId", userId, alloc);
    loginResp.AddMember("avatar_url", rapidjson::StringRef(avatarUrl.c_str()), alloc);

    rapidjson::Value onlineArray(rapidjson::kArrayType);
    for (const auto& [acc, uname, avatar] : onlineUsers) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("account", rapidjson::StringRef(acc.c_str()), alloc);
        obj.AddMember("username", rapidjson::StringRef(uname.c_str()), alloc);
        obj.AddMember("avatar_url", rapidjson::StringRef(avatar.c_str()), alloc);
        onlineArray.PushBack(obj, alloc);
    }
    loginResp.AddMember("online_users", onlineArray, alloc);

    rapidjson::StringBuffer loginBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> loginWriter(loginBuffer);
    loginResp.Accept(loginWriter);
    std::string loginMsg = loginBuffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, loginMsg);
}

void MessageRouter::doGroupMessage(int fd, std::shared_ptr<User> user, int groupId, const std::string& content) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    int userId = user->getUserId();
    std::string senderAccount = session_.getAccountByFd(fd);
    std::string senderUsername = session_.getUsernameByAccount(senderAccount);
    std::string senderAvatar = session_.getAvatar(senderAccount, "");

    int msgId = store_.saveMessage(userId, senderUsername, senderAvatar, groupId, true, content);
    if (msgId <= 0) {
        sendErrorResponse(fd, "Failed to save message");
        return;
    }

    auto members = group_.getGroupMembers(groupId);
    std::vector<int> onlineMembers;
    std::vector<int> offlineMembers;
    for (int uid : members) {
        std::string account = session_.getAccountByUserId(uid);
        if (account.empty()) {
            account = "test" + std::to_string(uid);
        }
        if (session_.isOnline(account)) {
            onlineMembers.push_back(uid);
        } else {
            offlineMembers.push_back(uid);
        }
    }

    if (!offlineMembers.empty()) {
        store_.pushToInbox(msgId, offlineMembers);
    }

    rapidjson::Document msgDoc;
    msgDoc.SetObject();
    auto& alloc = msgDoc.GetAllocator();
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
    std::string msg = buffer.GetString() + std::string("\n");

    if (onlineMembers.empty()) return;

    std::vector<std::string> onlineAccounts;
    onlineAccounts.reserve(onlineMembers.size());
    for (int uid : onlineMembers) {
        std::string account = session_.getAccountByUserId(uid);
        if (account.empty()) {
            account = "test" + std::to_string(uid);
        }
        onlineAccounts.push_back(account);
    }

    std::vector<int> fds = session_.getFdsByAccounts(onlineAccounts);
    auto users = ConnectionManager::instance().getUsers(fds);

    std::unordered_map<EventLoop*, std::vector<std::shared_ptr<User>>> threadUsers;
    for (size_t i = 0; i < fds.size(); ++i) {
        if (users[i] && fds[i] != -1) {
            threadUsers[users[i]->getLoop()].push_back(users[i]);
        }
    }

    for (auto& [loop, userList] : threadUsers) {
        loop->runInLoop([userList, msg]() {
            for (auto& targetUser : userList) {
                targetUser->send(msg.data(), msg.size());
            }
        });
    }
}

void MessageRouter::doSingleMessage(int fd, std::shared_ptr<User> user, const std::string& targetUsername, const std::string& content) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }

    int fromUserId = user->getUserId();
    std::string senderAccount = session_.getAccountByFd(fd);
    std::string senderUsername = session_.getUsernameByAccount(senderAccount);
    std::string senderAvatar = session_.getAvatar(senderAccount, "");

    std::string targetAccount = session_.getAccountByUsername(targetUsername);
    if (targetAccount.empty()) {
        sendErrorResponse(fd, "Target user not found");
        return;
    }
    int toUserId = session_.getUserIdByAccount(targetAccount);
    if (toUserId <= 0) {
        sendErrorResponse(fd, "Invalid target user");
        return;
    }
    int targetFd = session_.getFdByAccount(targetAccount);

    int msgId = store_.saveMessage(fromUserId, senderUsername, senderAvatar, toUserId, false, content);
    if (msgId <= 0) {
        sendErrorResponse(fd, "Failed to save message");
        return;
    }

    rapidjson::Document msgDoc;
    msgDoc.SetObject();
    auto& alloc = msgDoc.GetAllocator();
    msgDoc.AddMember("type", "single_message", alloc);
    msgDoc.AddMember("from", fromUserId, alloc);
    msgDoc.AddMember("from_username", rapidjson::StringRef(senderUsername.c_str()), alloc);
    msgDoc.AddMember("from_avatar", rapidjson::StringRef(senderAvatar.c_str()), alloc);
    msgDoc.AddMember("content", rapidjson::StringRef(content.c_str()), alloc);
    msgDoc.AddMember("msgId", msgId, alloc);
    msgDoc.AddMember("to", rapidjson::StringRef(targetUsername.c_str()), alloc);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    msgDoc.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");

    if (session_.isOnline(targetAccount)) {
        ConnectionManager::instance().sendToUser(targetFd, msg);
    } else {
        store_.pushToInbox(msgId, {toUserId});
    }

    rapidjson::Document receipt;
    receipt.SetObject();
    receipt.AddMember("type", "single_message_ack", receipt.GetAllocator());
    receipt.AddMember("status", "sent", receipt.GetAllocator());
    rapidjson::StringBuffer receiptBuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer2(receiptBuf);
    receipt.Accept(writer2);
    std::string receiptMsg = receiptBuf.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, receiptMsg);
}

void MessageRouter::doPullOffline(int fd, std::shared_ptr<User> user) {
    if (!user->isLoggedIn()) return;
    int userId = user->getUserId();
    auto ids = store_.pullOfflineMsgIds(userId);
    auto msgs = store_.getMessages(ids);

    for (auto& msg : msgs) {
        std::string account = session_.getAccountByUserId(msg.fromUserId);
        if (account.empty()) {
            account = "test" + std::to_string(msg.fromUserId);
        }
        std::string currentAvatar = session_.getAvatar(account, msg.fromAvatar);
        if (!currentAvatar.empty()) {
            msg.fromAvatar = currentAvatar;
        }
        std::string currentUsername = session_.getUsernameByAccount(account);
        if (!currentUsername.empty()) {
            msg.fromUsername = currentUsername;
        }
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "offline_messages", alloc);
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto& m : msgs) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("from", m.fromUserId, alloc);
        obj.AddMember("from_username", rapidjson::StringRef(m.fromUsername.c_str()), alloc);
        obj.AddMember("from_avatar", rapidjson::StringRef(m.fromAvatar.c_str()), alloc);
        obj.AddMember("type", m.type, alloc);
        obj.AddMember("content", rapidjson::StringRef(m.content.c_str()), alloc);
        obj.AddMember("msgId", m.msgId, alloc);
        if (m.type == 0) {
            obj.AddMember("groupId", m.toId, alloc);
        }
        arr.PushBack(obj, alloc);
    }
    resp.AddMember("messages", arr, alloc);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doHeartbeat(int fd, std::shared_ptr<User> user) {
     (void)user; 
    ConnectionManager::instance().sendToUser(fd, "{\"type\":\"pong\"}\n");
}

void MessageRouter::doGetOnlineUsers(int fd, std::shared_ptr<User> user) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    rapidjson::Document resp;
    resp.SetObject();
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "online_users", alloc);
    resp.AddMember("users", rapidjson::Value(rapidjson::kArrayType), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doLoadHistory(int fd, std::shared_ptr<User> user,
                                   const std::string& targetType,
                                   const std::string& targetId,
                                   int limit) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }

    LOG_INFO("[doLoadHistory] fd=" + std::to_string(fd) + ", targetType=" + targetType + ", targetId=" + targetId + ", limit=" + std::to_string(limit));

    std::vector<StoredMessage> history;
    if (targetType == "group") {
        int groupId = 0;
        try {
            groupId = std::stoi(targetId);
        } catch (const std::exception& e) {
            LOG_ERROR("[doLoadHistory] Invalid group id: " + targetId);
            sendErrorResponse(fd, "Invalid group id");
            return;
        }
        history = store_.loadHistory(0, groupId, true, limit);
        LOG_INFO("[doLoadHistory] group history count=" + std::to_string(history.size()) + " for groupId=" + std::to_string(groupId));
    } else if (targetType == "private") {
        std::string targetAccount = session_.getAccountByUsername(targetId);
        if (targetAccount.empty()) {
            LOG_ERROR("[doLoadHistory] Target user not found by username: " + targetId);
            sendErrorResponse(fd, "Target user not found");
            return;
        }
        int targetUserId = session_.getUserIdByAccount(targetAccount);
        if (targetUserId <= 0) {
            LOG_ERROR("[doLoadHistory] Failed to get user id for account: " + targetAccount);
            sendErrorResponse(fd, "Invalid target user");
            return;
        }
        int myUserId = user->getUserId();
        history = store_.loadHistory(myUserId, targetUserId, false, limit);
        LOG_INFO("[doLoadHistory] private history count=" + std::to_string(history.size()) + " between user " + std::to_string(myUserId) + " and " + std::to_string(targetUserId));
    } else {
        LOG_ERROR("[doLoadHistory] Invalid targetType: " + targetType);
        sendErrorResponse(fd, "Invalid target type");
        return;
    }

    for (auto& msg : history) {
        std::string account = session_.getAccountByUserId(msg.fromUserId);
        if (account.empty()) {
            account = "test" + std::to_string(msg.fromUserId);
        }
        std::string currentAvatar = session_.getAvatar(account, msg.fromAvatar);
        if (!currentAvatar.empty()) {
            msg.fromAvatar = currentAvatar;
        }
        std::string currentUsername = session_.getUsernameByAccount(account);
        if (!currentUsername.empty()) {
            msg.fromUsername = currentUsername;
        }
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "load_history", alloc);
    resp.AddMember("targetType", rapidjson::StringRef(targetType.c_str()), alloc);
    resp.AddMember("targetId", rapidjson::StringRef(targetId.c_str()), alloc);

    rapidjson::Value msgArray(rapidjson::kArrayType);
    for (const auto& msg : history) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("msgId", msg.msgId, alloc);
        obj.AddMember("from", msg.fromUserId, alloc);
        obj.AddMember("from_username", rapidjson::StringRef(msg.fromUsername.c_str()), alloc);
        obj.AddMember("from_avatar", rapidjson::StringRef(msg.fromAvatar.c_str()), alloc);
        obj.AddMember("content", rapidjson::StringRef(msg.content.c_str()), alloc);
        obj.AddMember("sendTime", static_cast<int64_t>(msg.sendTime), alloc);
        if (msg.type == 0) {
            obj.AddMember("groupId", msg.toId, alloc);
        }
        msgArray.PushBack(obj, alloc);
    }
    resp.AddMember("messages", msgArray, alloc);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doUpdateAvatar(int fd, std::shared_ptr<User> user, const std::string& avatarUrl) {
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
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "update_avatar", alloc);
    resp.AddMember("status", "success", alloc);
    resp.AddMember("avatar_url", rapidjson::StringRef(avatarUrl.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doUpdateUsername(int fd, std::shared_ptr<User> user, const std::string& newUsername) {
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
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "update_username", alloc);
    resp.AddMember("status", "success", alloc);
    resp.AddMember("username", rapidjson::StringRef(newUsername.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doRegister(int fd, const std::string& account,
                               const std::string& password,
                               const std::string& username) {
    std::string existingUsername;
    if (DBManager::getInstance().queryUsernameByAccount(account, existingUsername)) {
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

    GroupManager& gm = GroupManager::instance();
    for (int groupId = 1; groupId <= 3; ++groupId) {
        gm.addUserToGroup(groupId, userId);
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "register", alloc);
    resp.AddMember("status", "success", alloc);
    resp.AddMember("account", rapidjson::StringRef(account.c_str()), alloc);
    resp.AddMember("username", rapidjson::StringRef(username.c_str()), alloc);
    resp.AddMember("avatar_url", rapidjson::StringRef("https://disk.0voice.com/p/default_avatar.png"), alloc);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doGetConversations(int fd, std::shared_ptr<User> user) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    int userId = user->getUserId();
    std::string convKey = "user:conv:" + std::to_string(userId);

    auto allFields = RedisClient::instance().hgetall(convKey);
    std::unordered_map<std::string, std::tuple<std::string, std::string, int>> convData;

    for (const auto& [field, value] : allFields) {
        size_t colon = field.find(':');
        if (colon == std::string::npos) continue;
        std::string prefix = field.substr(0, colon);
        std::string suffix = field.substr(colon + 1);
        if (suffix == "lastMsg") {
            convData[prefix] = std::make_tuple(value, "", 0);
        } else if (suffix == "lastTime") {
            auto& data = convData[prefix];
            std::get<1>(data) = value;
        } else if (suffix == "unread") {
            auto& data = convData[prefix];
            try {
                std::get<2>(data) = std::stoi(value);
            } catch (...) {}
        }
    }

    rapidjson::Document resp;
    resp.SetObject();
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "conversations", alloc);
    rapidjson::Value convsArray(rapidjson::kArrayType);

    for (const auto& group : PRESET_GROUPS) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("type", "group", alloc);
        obj.AddMember("id", rapidjson::StringRef(std::to_string(group.id).c_str()), alloc);
        obj.AddMember("name", rapidjson::StringRef(group.name.c_str()), alloc);
        obj.AddMember("avatar", rapidjson::StringRef(group.avatar.c_str()), alloc);
        obj.AddMember("lastMsg", "", alloc);
        obj.AddMember("lastTime", 0, alloc);
        obj.AddMember("unread", 0, alloc);

        std::string prefix = "group:" + std::to_string(group.id);
        auto it = convData.find(prefix);
        if (it != convData.end()) {
            obj["lastMsg"] = rapidjson::StringRef(std::get<0>(it->second).c_str());
            if (!std::get<1>(it->second).empty()) {
                int64_t lastTime = std::stoll(std::get<1>(it->second));
                obj["lastTime"] = lastTime;
            }
            obj["unread"] = std::get<2>(it->second);
        }
        convsArray.PushBack(obj, alloc);
    }

    for (const auto& [prefix, data] : convData) {
        if (prefix.find("single:") != 0) continue;
        std::string peerId = prefix.substr(7);
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("type", "single", alloc);
        obj.AddMember("id", rapidjson::StringRef(peerId.c_str()), alloc);
        obj.AddMember("lastMsg", rapidjson::StringRef(std::get<0>(data).c_str()), alloc);
        int64_t lastTime = 0;
        if (!std::get<1>(data).empty()) {
            lastTime = std::stoll(std::get<1>(data));
        }
        obj.AddMember("lastTime", lastTime, alloc);
        obj.AddMember("unread", std::get<2>(data), alloc);

        int targetUserId = std::stoi(peerId);
        std::string account = session_.getAccountByUserId(targetUserId);
        if (account.empty()) {
            obj.AddMember("name", rapidjson::StringRef(peerId.c_str()), alloc);
            obj.AddMember("avatar", "", alloc);
        } else {
            std::string username = session_.getUsernameByAccount(account);
            std::string avatar = session_.getAvatar(account, "");
            if (username.empty()) username = account;
            obj.AddMember("name", rapidjson::StringRef(username.c_str()), alloc);
            obj.AddMember("avatar", rapidjson::StringRef(avatar.c_str()), alloc);
        }
        convsArray.PushBack(obj, alloc);
    }

    resp.AddMember("conversations", convsArray, alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = std::string(buffer.GetString()) + "\n";
    ConnectionManager::instance().sendToUser(fd, msg);
}

// 新增函数实现
void MessageRouter::doSearchUsers(int fd, std::shared_ptr<User> user, const std::string& keyword, int limit) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    auto results = session_.searchOnlineUsers(keyword, limit);
    rapidjson::Document resp;
    resp.SetObject();
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "search_users", alloc);
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto& [acc, uname, avatar] : results) {
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
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}

void MessageRouter::doGetUserProfile(int fd, std::shared_ptr<User> user, const std::string& targetAccount) {
    if (!user->isLoggedIn()) {
        sendErrorResponse(fd, "Not logged in");
        return;
    }
    auto profile = session_.getUserProfile(targetAccount);
    if (profile.account.empty()) {
        sendErrorResponse(fd, "User not found");
        return;
    }
    rapidjson::Document resp;
    resp.SetObject();
    auto& alloc = resp.GetAllocator();
    resp.AddMember("type", "user_profile", alloc);
    resp.AddMember("account", rapidjson::StringRef(profile.account.c_str()), alloc);
    resp.AddMember("username", rapidjson::StringRef(profile.username.c_str()), alloc);
    resp.AddMember("avatar_url", rapidjson::StringRef(profile.avatarUrl.c_str()), alloc);
    resp.AddMember("bio", rapidjson::StringRef(profile.bio.c_str()), alloc);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    resp.Accept(writer);
    std::string msg = buffer.GetString() + std::string("\n");
    ConnectionManager::instance().sendToUser(fd, msg);
}