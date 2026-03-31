#include "DBManager.hpp"
#include "ConnectionPool.hpp"
#include "Log.hpp"
#include <chrono>
#include <cstring>
#include <iostream>
#include <mysql/mysql.h>
#include <openssl/evp.h>
#include <sstream>
#include <iomanip>

DBManager &DBManager::getInstance() {
    static DBManager instance;
    return instance;
}

bool DBManager::connect(const std::string &host, const std::string &user,
                        const std::string &passwd, const std::string &db,
                        unsigned int port) {
    (void)host; (void)user; (void)passwd; (void)db; (void)port;
    MYSQL *conn = ConnectionPool::getInstance().getConnection();
    if (!conn) {
        error_ = "Failed to get connection from pool";
        return false;
    }
    ConnectionPool::getInstance().releaseConnection(conn);
    return true;
}

MYSQL *DBManager::getConnection() {
    return ConnectionPool::getInstance().getConnection();
}

void DBManager::releaseConnection(MYSQL *conn) {
    ConnectionPool::getInstance().releaseConnection(conn);
}

std::string DBManager::sha256(const std::string& str) {
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_len = 0;
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        LOG_ERROR("[sha256] EVP_MD_CTX_new failed");
        return "";
    }
    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
        LOG_ERROR("[sha256] EVP_DigestInit_ex failed");
        EVP_MD_CTX_free(ctx);
        return "";
    }
    if (EVP_DigestUpdate(ctx, str.c_str(), str.size()) != 1) {
        LOG_ERROR("[sha256] EVP_DigestUpdate failed");
        EVP_MD_CTX_free(ctx);
        return "";
    }
    if (EVP_DigestFinal_ex(ctx, hash, &hash_len) != 1) {
        LOG_ERROR("[sha256] EVP_DigestFinal_ex failed");
        EVP_MD_CTX_free(ctx);
        return "";
    }
    EVP_MD_CTX_free(ctx);

    std::stringstream ss;
    for (unsigned int i = 0; i < hash_len; ++i) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return ss.str();
}

bool DBManager::queryAccount(const std::string& account,
                             const std::string& password,
                             int& userId, std::string& username, std::string& avatarUrl) {
    MYSQL *conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return false;
    }

    std::string hashed = sha256(password);
    LOG_INFO("[queryAccount] hashed password: " + hashed);

    const char *sql = "SELECT id, username, avatar_url FROM chat_users WHERE account = ? AND password = ?";
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    MYSQL_BIND params[2];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_STRING;
    params[0].buffer = (char *)account.c_str();
    params[0].buffer_length = account.length();
    params[1].buffer_type = MYSQL_TYPE_STRING;
    params[1].buffer = (char *)hashed.c_str();
    params[1].buffer_length = hashed.length();

    if (mysql_stmt_bind_param(stmt, params) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    auto start = std::chrono::steady_clock::now();
    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    int id = 0;
    char usernameBuf[256] = {0};
    char avatarBuf[512] = {0};
    MYSQL_BIND results[3];
    memset(results, 0, sizeof(results));
    results[0].buffer_type = MYSQL_TYPE_LONG;
    results[0].buffer = &id;
    results[1].buffer_type = MYSQL_TYPE_STRING;
    results[1].buffer = usernameBuf;
    results[1].buffer_length = sizeof(usernameBuf);
    results[2].buffer_type = MYSQL_TYPE_STRING;
    results[2].buffer = avatarBuf;
    results[2].buffer_length = sizeof(avatarBuf);

    mysql_stmt_bind_result(stmt, results);
    mysql_stmt_store_result(stmt);

    bool found = false;
    if (mysql_stmt_fetch(stmt) == 0) {
        userId = id;
        username = usernameBuf;
        avatarUrl = avatarBuf;
        found = true;
        LOG_INFO("[queryAccount] login success for account: " + account + " id=" + std::to_string(id));
    } else {
        LOG_WARN("[queryAccount] login failed for account: " + account);
    }

    auto end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    if (ms > 100) {
        LOG_WARN("[DB] Slow query (" + std::to_string(ms) + "ms): " + sql);
    }

    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return found;
}

bool DBManager::queryUsernameByAccount(const std::string &account,
                                       std::string &username) {
    MYSQL *conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return false;
    }

    const char *sql = "SELECT username FROM chat_users WHERE account = ?";
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    MYSQL_BIND param;
    memset(&param, 0, sizeof(param));
    param.buffer_type = MYSQL_TYPE_STRING;
    param.buffer = (char *)account.c_str();
    param.buffer_length = account.length();

    if (mysql_stmt_bind_param(stmt, &param) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    char usernameBuf[256] = {0};
    MYSQL_BIND result;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_STRING;
    result.buffer = usernameBuf;
    result.buffer_length = sizeof(usernameBuf);

    mysql_stmt_bind_result(stmt, &result);
    mysql_stmt_store_result(stmt);

    bool found = false;
    if (mysql_stmt_fetch(stmt) == 0) {
        username = usernameBuf;
        found = true;
    }

    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return found;
}

bool DBManager::updateUsername(const std::string &account,
                               const std::string &new_username) {
    MYSQL *conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return false;
    }

    const char *sql = "UPDATE chat_users SET username = ? WHERE account = ?";
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    MYSQL_BIND params[2];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_STRING;
    params[0].buffer = (char *)new_username.c_str();
    params[0].buffer_length = new_username.length();
    params[1].buffer_type = MYSQL_TYPE_STRING;
    params[1].buffer = (char *)account.c_str();
    params[1].buffer_length = account.length();

    if (mysql_stmt_bind_param(stmt, params) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    bool success = mysql_stmt_affected_rows(stmt) > 0;
    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return success;
}

std::string DBManager::queryUserAvatar(const std::string &account) {
    MYSQL *conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return "";
    }

    const char *sql = "SELECT avatar_url FROM chat_users WHERE account = ?";
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return "";
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return "";
    }

    MYSQL_BIND param;
    memset(&param, 0, sizeof(param));
    param.buffer_type = MYSQL_TYPE_STRING;
    param.buffer = (char *)account.c_str();
    param.buffer_length = account.length();

    if (mysql_stmt_bind_param(stmt, &param) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return "";
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return "";
    }

    char avatarBuf[512] = {0};
    MYSQL_BIND result;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_STRING;
    result.buffer = avatarBuf;
    result.buffer_length = sizeof(avatarBuf);

    mysql_stmt_bind_result(stmt, &result);
    mysql_stmt_store_result(stmt);

    std::string avatar;
    if (mysql_stmt_fetch(stmt) == 0) {
        avatar = avatarBuf;
    }

    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return avatar;
}

bool DBManager::updateUserAvatar(const std::string &account,
                                 const std::string &avatarUrl) {
    MYSQL *conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return false;
    }

    const char *sql = "UPDATE chat_users SET avatar_url = ? WHERE account = ?";
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    MYSQL_BIND params[2];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_STRING;
    params[0].buffer = (char *)avatarUrl.c_str();
    params[0].buffer_length = avatarUrl.length();
    params[1].buffer_type = MYSQL_TYPE_STRING;
    params[1].buffer = (char *)account.c_str();
    params[1].buffer_length = account.length();

    if (mysql_stmt_bind_param(stmt, params) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    bool success = mysql_stmt_affected_rows(stmt) > 0;
    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return success;
}

std::string DBManager::getError() const {
    return error_;
}

// 修改：插入用户并返回自增ID
bool DBManager::insertUser(const std::string& account, const std::string& hashedPassword,
                           const std::string& username, int& userId) {
    MYSQL* conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        LOG_ERROR("[insertUser] getConnection failed");
        return false;
    }

    const char* sql = "INSERT INTO chat_users (account, password, username, avatar_url) VALUES (?, ?, ?, ?)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        LOG_ERROR("[insertUser] stmt init failed: " + error_);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        LOG_ERROR("[insertUser] prepare failed: " + error_);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    std::string defaultAvatar = "https://disk.0voice.com/p/default_avatar.png";
    MYSQL_BIND params[4];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_STRING;
    params[0].buffer = (char*)account.c_str();
    params[0].buffer_length = account.length();
    params[1].buffer_type = MYSQL_TYPE_STRING;
    params[1].buffer = (char*)hashedPassword.c_str();
    params[1].buffer_length = hashedPassword.length();
    params[2].buffer_type = MYSQL_TYPE_STRING;
    params[2].buffer = (char*)username.c_str();
    params[2].buffer_length = username.length();
    params[3].buffer_type = MYSQL_TYPE_STRING;
    params[3].buffer = (char*)defaultAvatar.c_str();
    params[3].buffer_length = defaultAvatar.length();

    if (mysql_stmt_bind_param(stmt, params) != 0) {
        error_ = mysql_stmt_error(stmt);
        LOG_ERROR("[insertUser] bind param failed: " + error_);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        LOG_ERROR("[insertUser] execute failed: " + error_);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    int affected = mysql_stmt_affected_rows(stmt);
    LOG_INFO("[insertUser] affected_rows = " + std::to_string(affected));
    if (affected <= 0) {
        error_ = "No rows inserted";
        LOG_ERROR("[insertUser] affected_rows = 0, possible constraint violation");
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    // 获取自增ID
    userId = mysql_stmt_insert_id(stmt);
    LOG_INFO("[insertUser] new user id = " + std::to_string(userId));

    if (mysql_commit(conn) != 0) {
        LOG_INFO("[insertUser] commit not needed or failed: " + std::string(mysql_error(conn)));
    }

    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return true;
}

bool DBManager::addGroupMember(int groupId, int userId) {
    MYSQL* conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return false;
    }

    const char* sql = "INSERT IGNORE INTO group_members (group_id, user_id) VALUES (?, ?)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    MYSQL_BIND params[2];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_LONG;
    params[0].buffer = &groupId;
    params[1].buffer_type = MYSQL_TYPE_LONG;
    params[1].buffer = &userId;

    if (mysql_stmt_bind_param(stmt, params) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    bool success = (mysql_stmt_affected_rows(stmt) > 0);
    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return success;
}

bool DBManager::removeGroupMember(int groupId, int userId) {
    MYSQL* conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return false;
    }

    const char* sql = "DELETE FROM group_members WHERE group_id = ? AND user_id = ?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    MYSQL_BIND params[2];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_LONG;
    params[0].buffer = &groupId;
    params[1].buffer_type = MYSQL_TYPE_LONG;
    params[1].buffer = &userId;

    if (mysql_stmt_bind_param(stmt, params) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    bool success = (mysql_stmt_affected_rows(stmt) > 0);
    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return success;
}

bool DBManager::isGroupMember(int groupId, int userId) {
    MYSQL* conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return false;
    }

    const char* sql = "SELECT 1 FROM group_members WHERE group_id = ? AND user_id = ? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    MYSQL_BIND params[2];
    memset(params, 0, sizeof(params));
    params[0].buffer_type = MYSQL_TYPE_LONG;
    params[0].buffer = &groupId;
    params[1].buffer_type = MYSQL_TYPE_LONG;
    params[1].buffer = &userId;

    if (mysql_stmt_bind_param(stmt, params) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return false;
    }

    MYSQL_BIND result;
    int exists = 0;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_LONG;
    result.buffer = &exists;
    mysql_stmt_bind_result(stmt, &result);
    mysql_stmt_store_result(stmt);
    bool found = (mysql_stmt_fetch(stmt) == 0);

    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return found;
}

std::vector<int> DBManager::getGroupMembersFromDB(int groupId) {
    std::vector<int> members;
    MYSQL* conn = getConnection();
    if (!conn) {
        error_ = "No database connection";
        return members;
    }

    const char* sql = "SELECT user_id FROM group_members WHERE group_id = ?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        error_ = mysql_error(conn);
        releaseConnection(conn);
        return members;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return members;
    }

    MYSQL_BIND param;
    memset(&param, 0, sizeof(param));
    param.buffer_type = MYSQL_TYPE_LONG;
    param.buffer = &groupId;
    if (mysql_stmt_bind_param(stmt, &param) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return members;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        error_ = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return members;
    }

    MYSQL_BIND result;
    int userId = 0;
    memset(&result, 0, sizeof(result));
    result.buffer_type = MYSQL_TYPE_LONG;
    result.buffer = &userId;
    mysql_stmt_bind_result(stmt, &result);
    mysql_stmt_store_result(stmt);

    while (mysql_stmt_fetch(stmt) == 0) {
        members.push_back(userId);
    }

    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return members;
}
std::vector<int> DBManager::getAllUserIds(int limit) {
    std::vector<int> ids;
    MYSQL* conn = getConnection();
    if (!conn) {
        LOG_ERROR("[getAllUserIds] 无法获取数据库连接");
        return ids;
    }

    const char* sql = "SELECT id FROM chat_users ORDER BY id LIMIT ?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        LOG_ERROR("[getAllUserIds] mysql_stmt_init 失败: " + std::string(mysql_error(conn)));
        releaseConnection(conn);
        return ids;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        LOG_ERROR("[getAllUserIds] mysql_stmt_prepare 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return ids;
    }

    MYSQL_BIND param;
    memset(&param, 0, sizeof(param));
    param.buffer_type = MYSQL_TYPE_LONG;
    param.buffer = &limit;
    if (mysql_stmt_bind_param(stmt, &param) != 0) {
        LOG_ERROR("[getAllUserIds] mysql_stmt_bind_param 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return ids;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        LOG_ERROR("[getAllUserIds] mysql_stmt_execute 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return ids;
    }

    MYSQL_BIND result;
    memset(&result, 0, sizeof(result));
    int userId = 0;
    result.buffer_type = MYSQL_TYPE_LONG;
    result.buffer = &userId;
    if (mysql_stmt_bind_result(stmt, &result) != 0) {
        LOG_ERROR("[getAllUserIds] mysql_stmt_bind_result 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return ids;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        LOG_ERROR("[getAllUserIds] mysql_stmt_store_result 失败: " + std::string(mysql_stmt_error(stmt)));
        mysql_stmt_close(stmt);
        releaseConnection(conn);
        return ids;
    }

    while (mysql_stmt_fetch(stmt) == 0) {
        ids.push_back(userId);
    }

    mysql_stmt_close(stmt);
    releaseConnection(conn);
    return ids;
}