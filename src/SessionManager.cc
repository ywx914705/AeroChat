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

void SessionManager::initEmpty() {}

bool SessionManager::login(int fd, const std::string& account, int userId,
                           const std::string& username, const std::string& avatarUrl) {
    if (fd <= 0) {
        LOG_ERROR("[SessionManager] 登录无效FD: " + std::to_string(fd));
        return false;
    }

    std::string oldAccount = getAccountByFd(fd);
    if (!oldAccount.empty()) {
        logout(fd);
    }

    // 写入 Redis（关键操作必须成功）
    if (!redis_.hset("fd_account", std::to_string(fd), account)) {
        LOG_ERROR("[SessionManager] Redis hset fd_account failed for account=" + account);
        return false;
    }
    if (!redis_.hset("user:" + account, "fd", std::to_string(fd))) {
        LOG_ERROR("[SessionManager] Redis hset user:fd failed for account=" + account);
        return false;
    }
    if (!redis_.hset("user:" + account, "user_id", std::to_string(userId))) {
        LOG_ERROR("[SessionManager] Redis hset user:user_id failed for account=" + account);
        return false;
    }
    if (!redis_.hset("user:" + account, "username", username)) {
        LOG_ERROR("[SessionManager] Redis hset user:username failed for account=" + account);
        return false;
    }
    if (!redis_.hset("user:" + account, "avatar", avatarUrl)) {
        LOG_ERROR("[SessionManager] Redis hset user:avatar failed for account=" + account);
        return false;
    }

    std::string userInfo = username + "|" + avatarUrl;
    if (!redis_.hset("online_users_info", account, userInfo)) {
        LOG_ERROR("[SessionManager] Redis hset online_users_info failed for account=" + account);
        return false;
    }

    if (!redis_.hset("username_to_account", username, account)) {
        LOG_ERROR("[SessionManager] Redis hset username_to_account failed for username=" + username);
        // 这个失败不影响在线状态，但记录错误
    }

    if (!redis_.expire("user:" + account, 7 * 24 * 3600)) {
        LOG_WARN("[SessionManager] Redis expire user:" + account + " failed");
    }

    // 写入内存
    {
        std::unique_lock lock(memoryMutex_);
        memoryOnline_[account] = {fd, userId, username, avatarUrl};
    }

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

    // 删除 Redis 记录（忽略删除失败，因为可能已经不存在）
    if (!redis_.hdel("fd_account", std::to_string(fd))) {
        LOG_WARN("[SessionManager] Redis hdel fd_account failed for fd=" + std::to_string(fd));
    }
    if (!redis_.del("user:" + account)) {
        LOG_WARN("[SessionManager] Redis del user:" + account + " failed");
    }
    if (!redis_.hdel("online_users_info", account)) {
        LOG_WARN("[SessionManager] Redis hdel online_users_info failed for account=" + account);
    }
    if (!username.empty()) {
        if (!redis_.hdel("username_to_account", username)) {
            LOG_WARN("[SessionManager] Redis hdel username_to_account failed for username=" + username);
        }
    }

    // 从内存删除
    {
        std::unique_lock lock(memoryMutex_);
        memoryOnline_.erase(account);
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
    // 优先查内存（性能）
    {
        std::shared_lock lock(memoryMutex_);
        if (memoryOnline_.find(account) != memoryOnline_.end())
            return true;
    }
    // 后备查 Redis
    return redis_.hexists("online_users_info", account);
}

std::string SessionManager::getAccountByFd(int fd) const {
    return redis_.hget("fd_account", std::to_string(fd));
}

int SessionManager::getFdByAccount(const std::string& account) const {
    // 优先从内存获取
    {
        std::shared_lock lock(memoryMutex_);
        auto it = memoryOnline_.find(account);
        if (it != memoryOnline_.end())
            return it->second.fd;
    }
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
    if (!redis_.hset("user:" + account, "avatar", avatarUrl)) {
        LOG_ERROR("[SessionManager] setAvatar: hset user:avatar failed for account=" + account);
        return;
    }
    std::string userInfo = getUsernameByAccount(account) + "|" + avatarUrl;
    if (!redis_.hset("online_users_info", account, userInfo)) {
        LOG_ERROR("[SessionManager] setAvatar: hset online_users_info failed for account=" + account);
    }
    redis_.expire("user:" + account, 7 * 24 * 3600);
    // 更新内存中的 avatar
    {
        std::unique_lock lock(memoryMutex_);
        auto it = memoryOnline_.find(account);
        if (it != memoryOnline_.end()) {
            it->second.avatar = avatarUrl;
        }
    }
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
        if (!redis_.hset("user:" + account, "username", new_username)) {
            LOG_ERROR("[SessionManager] updateUsername: hset username failed for account=" + account);
        }
        std::string avatar = redis_.hget("user:" + account, "avatar");
        std::string userInfo = new_username + "|" + avatar;
        if (!redis_.hset("online_users_info", account, userInfo)) {
            LOG_ERROR("[SessionManager] updateUsername: hset online_users_info failed for account=" + account);
        }
        if (!redis_.hset("username_to_account", new_username, account)) {
            LOG_ERROR("[SessionManager] updateUsername: hset username_to_account failed for new_username=" + new_username);
        }
        redis_.expire("user:" + account, 7 * 24 * 3600);
        // 更新内存中的 username
        {
            std::unique_lock lock(memoryMutex_);
            auto it = memoryOnline_.find(account);
            if (it != memoryOnline_.end()) {
                it->second.username = new_username;
            }
        }
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

    std::vector<int> result;
    result.reserve(accounts.size());
    // 优先从内存批量获取
    {
        std::shared_lock lock(memoryMutex_);
        for (const auto& acc : accounts) {
            auto it = memoryOnline_.find(acc);
            if (it != memoryOnline_.end()) {
                result.push_back(it->second.fd);
            } else {
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
    if (!redis_.publish("online_updates", msg)) {
        LOG_WARN("[SessionManager] publishOnlineUpdate failed for action=" + action + ", account=" + account);
    }
}

std::vector<OnlineUserInfo> SessionManager::searchOnlineUsers(const std::string& keyword, int limit) const {
    std::vector<OnlineUserInfo> result;
    if (keyword.empty() || limit <= 0) return result;

    auto infoMap = redis_.hgetall("online_users_info");
    for (const auto& [account, value] : infoMap) {
        size_t sep = value.find('|');
        std::string username = (sep != std::string::npos) ? value.substr(0, sep) : value;
        if (account.find(keyword) == 0 || username.find(keyword) == 0) {
            std::string avatar = (sep != std::string::npos) ? value.substr(sep + 1) : "";
            result.push_back({account, username, avatar});
            if (result.size() >= static_cast<size_t>(limit)) break;
        }
    }
    return result;
}

std::vector<OnlineUserInfo> SessionManager::searchAllUsers(const std::string& keyword, int limit) const {
    std::vector<OnlineUserInfo> result;
    if (keyword.empty() || limit <= 0) return result;

    MYSQL* conn = DBManager::getInstance().getConnection();
    if (!conn) {
        LOG_ERROR("[SessionManager] searchAllUsers 无法获取数据库连接");
        return result;
    }

    const char* sql = "SELECT account, username, avatar_url FROM chat_users WHERE account LIKE ? OR username LIKE ? LIMIT ?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        DBManager::getInstance().releaseConnection(conn);
        return result;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        LOG_ERROR("[SessionManager] searchAllUsers prepare failed: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return result;
    }

    std::string pattern = keyword + "%";
    MYSQL_BIND params[3];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_STRING;
    params[0].buffer = (char*)pattern.c_str();
    params[0].buffer_length = pattern.length();
    params[1] = params[0];
    params[2].buffer_type = MYSQL_TYPE_LONG;
    params[2].buffer = &limit;

    if (mysql_stmt_bind_param(stmt, params) != 0) {
        LOG_ERROR("[SessionManager] searchAllUsers bind param failed: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return result;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        LOG_ERROR("[SessionManager] searchAllUsers execute failed: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return result;
    }

    MYSQL_BIND result_bind[3];
    memset(result_bind, 0, sizeof(result_bind));
    char accountBuf[64] = {0}, usernameBuf[64] = {0}, avatarBuf[256] = {0};
    result_bind[0].buffer_type = MYSQL_TYPE_STRING;
    result_bind[0].buffer = accountBuf;
    result_bind[0].buffer_length = sizeof(accountBuf);
    result_bind[1].buffer_type = MYSQL_TYPE_STRING;
    result_bind[1].buffer = usernameBuf;
    result_bind[1].buffer_length = sizeof(usernameBuf);
    result_bind[2].buffer_type = MYSQL_TYPE_STRING;
    result_bind[2].buffer = avatarBuf;
    result_bind[2].buffer_length = sizeof(avatarBuf);
    if (mysql_stmt_bind_result(stmt, result_bind) != 0) {
        LOG_ERROR("[SessionManager] searchAllUsers bind result failed: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return result;
    }

    mysql_stmt_store_result(stmt);
    while (mysql_stmt_fetch(stmt) == 0) {
        result.push_back({accountBuf, usernameBuf, avatarBuf});
    }

    mysql_stmt_close(stmt);
    DBManager::getInstance().releaseConnection(conn);
    return result;
}

SessionManager::UserProfile SessionManager::getUserProfile(const std::string& account) const {
    UserProfile profile;
    profile.account = account;
    profile.username = getUsernameByAccount(account);
    profile.avatarUrl = getAvatar(account, "");
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

// 新增：根据账号或用户名获取真实账号（支持数据库回查）
std::string SessionManager::getAccountByAccountOrUsername(const std::string& input) const {
    if (input.empty()) return "";

    // 1. 先查 Redis（快速路径）
    bool isAllDigit = !input.empty() && std::all_of(input.begin(), input.end(), ::isdigit);
    if (isAllDigit) {
        std::string check = redis_.hget("user:" + input, "user_id");
        if (!check.empty()) return input;
    } else {
        std::string account = redis_.hget("username_to_account", input);
        if (!account.empty()) return account;
    }

    // 2. Redis 未命中，查数据库
    MYSQL* conn = DBManager::getInstance().getConnection();
    if (!conn) {
        LOG_ERROR("[getAccountByAccountOrUsername] 无法获取数据库连接");
        return "";
    }

    const char* sql = "SELECT account FROM chat_users WHERE account = ? OR username = ? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        mysql_stmt_close(stmt);
        DBManager::getInstance().releaseConnection(conn);
        return "";
    }

    MYSQL_BIND params[2];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_STRING;
    params[0].buffer = (char*)input.c_str();
    params[0].buffer_length = input.length();
    params[1] = params[0];

    if (mysql_stmt_bind_param(stmt, params) != 0) {
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

    std::string foundAccount;
    if (mysql_stmt_fetch(stmt) == 0) {
        foundAccount = accountBuf;
    }

    mysql_stmt_close(stmt);
    DBManager::getInstance().releaseConnection(conn);
    return foundAccount;
}