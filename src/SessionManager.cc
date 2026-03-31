#include "SessionManager.hpp"
#include "DBManager.hpp"
#include "Log.hpp"
#include <fcntl.h>
#include <errno.h>
#include <iostream>
#include <functional>
#include <tuple>
#include <cstring>
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

SessionManager& SessionManager::instance() {
    static SessionManager instance;
    return instance;
}

SessionManager::SessionManager() : redis_(RedisClient::instance()) {}

bool SessionManager::init(const std::string& db_host, const std::string& db_user,
                          const std::string& db_passwd, const std::string& db_name,
                          unsigned int db_port) {
    return DBManager::getInstance().connect(db_host, db_user, db_passwd, db_name, db_port);
}

void SessionManager::init() {}

bool SessionManager::login(int fd, const std::string& account, int userId,
                           const std::string& username, const std::string& avatarUrl) {
    if (fd <= 0 || (fcntl(fd, F_GETFD) == -1 && errno == EBADF)) {
        LOG_ERROR("[SessionManager] 登录无效FD: " + std::to_string(fd));
        return false;
    }

    std::string oldAccount = getAccountByFd(fd);
    if (!oldAccount.empty()) {
        logout(fd);
    }

    redis_.hset("fd_account", std::to_string(fd), account);
    redis_.hset("user:" + account, "fd", std::to_string(fd));
    redis_.hset("user:" + account, "user_id", std::to_string(userId));
    redis_.hset("user:" + account, "username", username);
    redis_.hset("user:" + account, "avatar", avatarUrl);
    // 存储在线用户信息：account -> "username|avatar"
    std::string userInfo = username + "|" + avatarUrl;
    redis_.hset("online_users_info", account, userInfo);
    redis_.hset("username_to_account", username, account);

    redis_.expire("user:" + account, 7 * 24 * 3600);

    LOG_INFO("[SessionManager] login success: account=" + account +
             ", userId=" + std::to_string(userId) +
             ", fd=" + std::to_string(fd));

    publishOnlineUpdate("join", account, username, avatarUrl);
    return true;
}

void SessionManager::logout(int fd) {
    std::string account = getAccountByFd(fd);
    if (account.empty()) return;

    std::string username = redis_.hget("user:" + account, "username");
    std::string avatar = redis_.hget("user:" + account, "avatar");

    redis_.hdel("fd_account", std::to_string(fd));
    redis_.del("user:" + account);
    redis_.hdel("online_users_info", account);
    if (!username.empty()) {
        redis_.hdel("username_to_account", username);
    }

    LOG_INFO("[SessionManager] logout: account=" + account + ", fd=" + std::to_string(fd));

    if (!username.empty()) {
        publishOnlineUpdate("leave", account, username, avatar);
    }
}

bool SessionManager::isOnline(int fd) const {
    return !getAccountByFd(fd).empty();
}

bool SessionManager::isOnline(const std::string& account) const {
    return redis_.hexists("online_users_info", account);
}

std::string SessionManager::getAccountByFd(int fd) const {
    return redis_.hget("fd_account", std::to_string(fd));
}

int SessionManager::getFdByAccount(const std::string& account) const {
    std::string fdStr = redis_.hget("user:" + account, "fd");
    if (fdStr.empty()) return -1;
    int fd = std::stoi(fdStr);
    if (fd > 0 && (fcntl(fd, F_GETFD) == -1 && errno == EBADF)) {
        const_cast<SessionManager*>(this)->logout(fd);
        return -1;
    }
    return fd;
}

std::string SessionManager::getUsernameByAccount(const std::string& account) const {
    std::string username = redis_.hget("user:" + account, "username");
    if (!username.empty()) {
        return username;
    }

    MYSQL* conn = DBManager::getInstance().getConnection();
    if (!conn) {
        LOG_ERROR("[getUsernameByAccount] 无法获取数据库连接");
        return "";
    }

    const char* sql = "SELECT username FROM chat_users WHERE account = ?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        LOG_ERROR("[getUsernameByAccount] mysql_stmt_init 失败: " + std::string(mysql_error(conn)));
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        LOG_ERROR("[getUsernameByAccount] mysql_stmt_prepare 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }

    MYSQL_BIND param;
    memset(&param, 0, sizeof(param));
    param.buffer_type = MYSQL_TYPE_STRING;
    param.buffer = (char*)account.c_str();
    param.buffer_length = account.length();
    if (mysql_stmt_bind_param(stmt, &param) != 0) {
        LOG_ERROR("[getUsernameByAccount] mysql_stmt_bind_param 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }

    if (mysql_stmt_execute(stmt) != 0) {
        LOG_ERROR("[getUsernameByAccount] mysql_stmt_execute 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }

    MYSQL_BIND result;
    char nameBuf[256] = {0};
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_STRING;
    result.buffer = nameBuf;
    result.buffer_length = sizeof(nameBuf);
    if (mysql_stmt_bind_result(stmt, &result) != 0) {
        LOG_ERROR("[getUsernameByAccount] mysql_stmt_bind_result 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }

    if (mysql_stmt_fetch(stmt) == 0) {
        username = nameBuf;
        const_cast<RedisClient&>(redis_).hset("user:" + account, "username", username);
        const_cast<RedisClient&>(redis_).expire("user:" + account, 7 * 24 * 3600);
    } else {
        LOG_DEBUG("[getUsernameByAccount] 未找到用户: " + account);
    }

    mysql_stmt_close(stmt);
    DBManager::getInstance().releaseConnection(conn);
    return username;
}

std::string SessionManager::getUsernameByFd(int fd) const {
    std::string account = getAccountByFd(fd);
    if (account.empty()) return "";
    return getUsernameByAccount(account);
}

std::string SessionManager::getAccountByUsername(const std::string& username) const {
    return redis_.hget("username_to_account", username);
}

int SessionManager::getUserIdByAccount(const std::string& account) const {
    if (account.empty()) return -1;
    std::string userIdStr = redis_.hget("user:" + account, "user_id");
    if (!userIdStr.empty()) {
        try {
            return std::stoi(userIdStr);
        } catch (...) {}
    }
    MYSQL* conn = DBManager::getInstance().getConnection();
    if (!conn) return -1;
    std::string sql = "SELECT id FROM chat_users WHERE account = ?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        DBManager::getInstance().releaseConnection(conn);
        return -1;
    }
    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length()) != 0) {
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return -1;
    }
    MYSQL_BIND param;
    memset(&param, 0, sizeof(param));
    param.buffer_type = MYSQL_TYPE_STRING;
    param.buffer = (char*)account.c_str();
    param.buffer_length = account.length();
    if (mysql_stmt_bind_param(stmt, &param) != 0) {
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return -1;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return -1;
    }
    int userId = -1;
    MYSQL_BIND result;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_LONG;
    result.buffer = &userId;
    mysql_stmt_bind_result(stmt, &result);
    mysql_stmt_store_result(stmt);
    if (mysql_stmt_fetch(stmt) == 0) {
        const_cast<RedisClient&>(redis_).hset("user:" + account, "user_id", std::to_string(userId));
        const_cast<RedisClient&>(redis_).expire("user:" + account, 7 * 24 * 3600);
    }
    mysql_stmt_close(stmt);
    DBManager::getInstance().releaseConnection(conn);
    return userId;
}

std::string SessionManager::getAccountByUserId(int userId) const {
    MYSQL* conn = DBManager::getInstance().getConnection();
    if (!conn) return "";
    std::string sql = "SELECT account FROM chat_users WHERE id = ?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }
    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length()) != 0) {
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }
    MYSQL_BIND param;
    memset(&param, 0, sizeof(param));
    param.buffer_type = MYSQL_TYPE_LONG;
    param.buffer = (void*)&userId;
    if (mysql_stmt_bind_param(stmt, &param) != 0) {
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }
    if (mysql_stmt_execute(stmt) != 0) {
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }
    char accountBuf[64] = {0};
    MYSQL_BIND result;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_STRING;
    result.buffer = accountBuf;
    result.buffer_length = sizeof(accountBuf);
    mysql_stmt_bind_result(stmt, &result);
    mysql_stmt_store_result(stmt);
    std::string account;
    if (mysql_stmt_fetch(stmt) == 0) {
        account = accountBuf;
    }
    mysql_stmt_close(stmt);
    DBManager::getInstance().releaseConnection(conn);
    return account;
}

void SessionManager::setAvatar(const std::string& account, const std::string& avatarUrl) {
    if (account.empty()) return;
    redis_.hset("user:" + account, "avatar", avatarUrl);
    std::string userInfo = getUsernameByAccount(account) + "|" + avatarUrl;
    redis_.hset("online_users_info", account, userInfo);
    redis_.expire("user:" + account, 7 * 24 * 3600);
}

std::string SessionManager::getAvatar(const std::string& account, const std::string& defaultAvatar) const {
    std::string avatar = redis_.hget("user:" + account, "avatar");
    if (avatar.empty() && !defaultAvatar.empty()) {
        return defaultAvatar;
    }
    return avatar.empty() ? defaultAvatar : avatar;
}

std::string SessionManager::getAvatarByFd(int fd, const std::string& defaultAvatar) const {
    std::string account = getAccountByFd(fd);
    return getAvatar(account, defaultAvatar);
}

bool SessionManager::updateUsername(int fd, const std::string& new_username) {
    std::string account = getAccountByFd(fd);
    if (account.empty()) return false;

    std::string oldUsername = redis_.hget("user:" + account, "username");

    if (!oldUsername.empty()) {
        redis_.hdel("username_to_account", oldUsername);
    }

    bool db_ok = DBManager::getInstance().updateUsername(account, new_username);
    if (db_ok) {
        redis_.hset("user:" + account, "username", new_username);
        std::string avatar = redis_.hget("user:" + account, "avatar");
        std::string userInfo = new_username + "|" + avatar;
        redis_.hset("online_users_info", account, userInfo);
        redis_.hset("username_to_account", new_username, account);
        redis_.expire("user:" + account, 7 * 24 * 3600);
    } else {
        if (!oldUsername.empty()) {
            redis_.hset("username_to_account", oldUsername, account);
        }
    }
    return db_ok;
}

std::vector<std::tuple<std::string, std::string, std::string>> SessionManager::getAllOnlineUserInfos() const {
    std::vector<std::tuple<std::string, std::string, std::string>> result;
    auto infoMap = redis_.hgetall("online_users_info");
    for (const auto& [account, value] : infoMap) {
        size_t sep = value.find('|');
        std::string username = (sep != std::string::npos) ? value.substr(0, sep) : value;
        std::string avatar = (sep != std::string::npos) ? value.substr(sep + 1) : "";
        result.emplace_back(account, username, avatar);
    }
    return result;
}

int SessionManager::getOnlineCount() const {
    return redis_.hlen("online_users_info");
}

std::vector<int> SessionManager::getFdsByAccounts(const std::vector<std::string>& accounts) const {
    if (accounts.empty()) return {};

    std::vector<std::string> keys;
    keys.reserve(accounts.size());
    for (const auto& acc : accounts) {
        keys.push_back("user:" + acc);
    }

    std::vector<std::string> fdStrs = redis_.multiHget(keys, "fd");

    std::vector<int> result;
    result.reserve(fdStrs.size());
    for (const auto& fdStr : fdStrs) {
        if (fdStr.empty()) {
            result.push_back(-1);
        } else {
            try {
                int fd = std::stoi(fdStr);
                result.push_back(fd);
            } catch (...) {
                result.push_back(-1);
            }
        }
    }
    return result;
}

void SessionManager::publishOnlineUpdate(const std::string& action, const std::string& account,
                                         const std::string& username, const std::string& avatarUrl) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();
    doc.AddMember("type", "online_update", alloc);
    doc.AddMember("action", rapidjson::StringRef(action.c_str()), alloc);
    doc.AddMember("account", rapidjson::StringRef(account.c_str()), alloc);
    doc.AddMember("username", rapidjson::StringRef(username.c_str()), alloc);
    doc.AddMember("avatar_url", rapidjson::StringRef(avatarUrl.c_str()), alloc);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    doc.Accept(writer);
    std::string msg = buf.GetString();
    redis_.publish("online_updates", msg);
}

std::vector<std::tuple<std::string, std::string, std::string>> SessionManager::searchOnlineUsers(const std::string& keyword, int limit) const {
    std::vector<std::tuple<std::string, std::string, std::string>> result;
    if (keyword.empty() || limit <= 0) return result;

    auto infoMap = redis_.hgetall("online_users_info");
    for (const auto& [account, value] : infoMap) {
        size_t sep = value.find('|');
        std::string username = (sep != std::string::npos) ? value.substr(0, sep) : value;
        // 不区分大小写的前缀匹配（简单实现，可根据需要优化）
        if (account.find(keyword) == 0 || username.find(keyword) == 0) {
            std::string avatar = (sep != std::string::npos) ? value.substr(sep + 1) : "";
            result.emplace_back(account, username, avatar);
            if (result.size() >= static_cast<size_t>(limit)) break;
        }
    }
    return result;
}

SessionManager::UserProfile SessionManager::getUserProfile(const std::string& account) const {
    UserProfile profile;
    profile.account = account;
    profile.username = getUsernameByAccount(account);
    profile.avatarUrl = getAvatar(account, "");
    // 从数据库获取个人简介（假设表中有 bio 字段）
    MYSQL* conn = DBManager::getInstance().getConnection();
    if (conn) {
        const char* sql = "SELECT bio FROM chat_users WHERE account = ?";
        MYSQL_STMT* stmt = mysql_stmt_init(conn);
        if (stmt && mysql_stmt_prepare(stmt, sql, strlen(sql)) == 0) {
            MYSQL_BIND param;
            memset(&param, 0, sizeof(param));
            param.buffer_type = MYSQL_TYPE_STRING;
            param.buffer = (char*)account.c_str();
            param.buffer_length = account.length();
            mysql_stmt_bind_param(stmt, &param);
            if (mysql_stmt_execute(stmt) == 0) {
                char bioBuf[256] = {0};
                MYSQL_BIND result;
                memset(&result, 0, sizeof(result));
                result.buffer_type = MYSQL_TYPE_STRING;
                result.buffer = bioBuf;
                result.buffer_length = sizeof(bioBuf);
                mysql_stmt_bind_result(stmt, &result);
                if (mysql_stmt_fetch(stmt) == 0) {
                    profile.bio = bioBuf;
                }
            }
        }
        if (stmt) mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
    }
    return profile;
}