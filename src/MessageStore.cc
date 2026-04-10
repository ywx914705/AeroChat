#include "MessageStore.hpp"
#include "AeroQueue.hpp"
#include "ConnectionManager.hpp"
#include "Log.hpp"
#include "MessageRouter.hpp"
#include "SessionManager.hpp"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <cstring>
#include <iostream>
#include <mysql/mysql.h>
#include <sstream>
#include <algorithm>   // for std::all_of
#include <cctype>      // for ::isdigit

MessageStore &MessageStore::instance() {
    static MessageStore instance;
    return instance;
}

MessageStore::MessageStore()
    : db_(DBManager::getInstance()), redis_(RedisClient::instance()),
      group_(GroupManager::instance()) {}

void MessageStore::bindParam(MYSQL_BIND *bind, const std::string &value) {
    bind->buffer_type = MYSQL_TYPE_STRING;
    bind->buffer = (char *)value.c_str();
    bind->buffer_length = value.length();
    bind->is_null = 0;
}

// 群聊保存消息（修复：存储账号而非用户ID）
int MessageStore::saveMessage(int fromUserId, const std::string &fromUsername,
                              const std::string &fromAvatar, int toId,
                              bool isGroup, const std::string &content) {
    if (!isGroup) {
        // 私聊消息请使用另一个重载版本
        setError("saveMessage(group) called for private message, use the other overload");
        return -1;
    }

    // FIXED: 获取发送者的真实账号
    std::string fromAccount = SessionManager::instance().getAccountByUserId(fromUserId);
    if (fromAccount.empty()) {
        setError("Cannot find account for fromUserId: " + std::to_string(fromUserId));
        return -1;
    }

    MYSQL *conn = db_.getConnection();
    if (!conn) {
        setError("Failed to get DB connection");
        return -1;
    }

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        setError(mysql_error(conn));
        db_.releaseConnection(conn);
        return -1;
    }

    const char *sql = "INSERT INTO chat_group_messages (from_account, from_username, "
                      "avatar_url, group_id, content, create_time) VALUES (?, ?, ?, ?, ?, NOW())";
    MYSQL_BIND params[5];
    memset(params, 0, sizeof(params));
    int paramCount = 0;

    std::string toIdStr = std::to_string(toId);

    bindParam(&params[paramCount++], fromAccount);      // 存储账号
    bindParam(&params[paramCount++], fromUsername);
    bindParam(&params[paramCount++], fromAvatar);
    bindParam(&params[paramCount++], toIdStr);
    bindParam(&params[paramCount++], content);

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0 ||
        mysql_stmt_bind_param(stmt, params) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        setError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return -1;
    }

    int msgId = mysql_stmt_insert_id(stmt);
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);

    LOG_INFO("[MessageStore] saveMessage(group) success, msgId=" +
             std::to_string(msgId) + " groupId=" + toIdStr);

    // 更新会话摘要
    time_t now = time(nullptr);
    std::string convId = std::to_string(toId);
    updateConversationSummary(fromUserId, "group", convId, content, now, false);
    auto members = group_.getGroupMembers(toId);
    for (int uid : members) {
        if (uid != fromUserId) {
            updateConversationSummary(uid, "group", convId, content, now, true);
        }
    }

    return msgId;
}

// 私聊保存消息（保持不变）
int MessageStore::saveMessage(int fromUserId, const std::string &fromUsername,
                              const std::string &fromAvatar,
                              const std::string &toAccount,
                              const std::string &content) {
    MYSQL *conn = db_.getConnection();
    if (!conn) {
        setError("Failed to get DB connection");
        return -1;
    }

    std::string fromAccount = SessionManager::instance().getAccountByUserId(fromUserId);
    if (fromAccount.empty()) {
        setError("Cannot find account for fromUserId");
        db_.releaseConnection(conn);
        return -1;
    }

    const char *sql = "INSERT INTO chat_single_messages "
                      "(from_account, to_account, from_username, avatar_url, content, create_time) "
                      "VALUES (?, ?, ?, ?, ?, NOW())";
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        setError(mysql_error(conn));
        db_.releaseConnection(conn);
        return -1;
    }

    MYSQL_BIND params[5];
    memset(params, 0, sizeof(params));
    int idx = 0;
    bindParam(&params[idx++], fromAccount);
    bindParam(&params[idx++], toAccount);
    bindParam(&params[idx++], fromUsername);
    bindParam(&params[idx++], fromAvatar);
    bindParam(&params[idx++], content);

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0 ||
        mysql_stmt_bind_param(stmt, params) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        setError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return -1;
    }

    int msgId = mysql_stmt_insert_id(stmt);
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);

    LOG_INFO("[MessageStore] saveMessage(single) success, msgId=" +
             std::to_string(msgId) + " from=" + fromAccount + " to=" + toAccount);

    // 更新会话摘要
    time_t now = time(nullptr);
    updateConversationSummary(fromUserId, "single", toAccount, content, now, false);
    int toUserId = SessionManager::instance().getUserIdByAccount(toAccount);
    if (toUserId > 0) {
        updateConversationSummary(toUserId, "single", fromAccount, content, now, true);
    }

    return msgId;
}

//离线消息
void MessageStore::pushToInbox(int msgId, const std::vector<int> &targetUserIds) {
    std::string msgIdStr = std::to_string(msgId);
    for (int uid : targetUserIds) {
        std::string key = INBOX_KEY_PREFIX + std::to_string(uid);
        redis_.rpush(key, msgIdStr);
        LOG_INFO("[MessageStore] pushToInbox: msg=" + std::to_string(msgId) +
                 " to user=" + std::to_string(uid));
    }
}

std::vector<int> MessageStore::pullOfflineMsgIds(int userId) {
    std::string key = INBOX_KEY_PREFIX + std::to_string(userId);
    auto ids = redis_.lrange(key, 0, -1);
    redis_.del(key);
    std::vector<int> result;
    for (const auto &id : ids) {
        result.push_back(std::stoi(id));
    }
    LOG_INFO("[MessageStore] pullOfflineMsgIds user=" + std::to_string(userId) +
             " count=" + std::to_string(result.size()));
    return result;
}

std::vector<StoredMessage> MessageStore::getMessages(const std::vector<int> &msgIds) {
    std::vector<StoredMessage> result;
    if (msgIds.empty()) return result;

    std::string ids;
    for (size_t i = 0; i < msgIds.size(); ++i) {
        if (i > 0) ids += ",";
        ids += std::to_string(msgIds[i]);
    }

    std::string sql = "SELECT id, from_account, from_username, avatar_url, "
                      "group_id, content, UNIX_TIMESTAMP(create_time), 0 as type "
                      "FROM chat_group_messages WHERE id IN (" + ids + ")"
                      " UNION ALL "
                      "SELECT id, from_account, from_username, avatar_url, "
                      "to_account, content, UNIX_TIMESTAMP(create_time), 1 as "
                      "type FROM chat_single_messages WHERE id IN (" + ids + ")";

    MYSQL *conn = db_.getConnection();
    if (!conn) {
        setError("No DB connection");
        return result;
    }

    if (mysql_query(conn, sql.c_str()) != 0) {
        setError(mysql_error(conn));
        db_.releaseConnection(conn);
        return result;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
        setError(mysql_error(conn));
        db_.releaseConnection(conn);
        return result;
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res))) {
        StoredMessage msg;
        msg.msgId = std::stoi(row[0]);
        std::string fromAccount = row[1] ? row[1] : "";
        msg.fromUserId = SessionManager::instance().getUserIdByAccount(fromAccount);
        // 兼容旧数据：如果 fromAccount 是纯数字且查询失败，直接转换为 int
        if (msg.fromUserId <= 0 && !fromAccount.empty() &&
            std::all_of(fromAccount.begin(), fromAccount.end(), ::isdigit)) {
            msg.fromUserId = std::stoi(fromAccount);
        }
        msg.fromUsername = row[2] ? row[2] : "";
        msg.fromAvatar = row[3] ? row[3] : "";
        if (row[7] && std::stoi(row[7]) == 0) {
            // 群聊消息
            msg.toId = (row[4] ? std::stoi(row[4]) : 0);
        } else {
            // 私聊消息：toId 存储对方用户ID
            std::string toAccount = row[4] ? row[4] : "";
            msg.toId = SessionManager::instance().getUserIdByAccount(toAccount);
            if (msg.toId <= 0 && !toAccount.empty() &&
                std::all_of(toAccount.begin(), toAccount.end(), ::isdigit)) {
                msg.toId = std::stoi(toAccount);
            }
        }
        msg.content = row[5] ? row[5] : "";
        msg.sendTime = std::stol(row[6]);
        msg.type = row[7] ? std::stoi(row[7]) : 0;
        result.push_back(msg);
    }
    mysql_free_result(res);
    db_.releaseConnection(conn);
    return result;
}

// 历史消息（私聊，使用账号）
std::vector<StoredMessage> MessageStore::loadHistoryPaginated(const std::string &myAccount,
                                                              const std::string &peerAccount,
                                                              int limit, int offset) {
    std::vector<StoredMessage> result;
    MYSQL *conn = db_.getConnection();
    if (!conn) {
        setError("No DB connection");
        return result;
    }

    const char *sql = "SELECT id, from_account, from_username, avatar_url, to_account, content, "
                      "UNIX_TIMESTAMP(create_time) as send_time, 1 as type "
                      "FROM chat_single_messages "
                      "WHERE (from_account = ? AND to_account = ?) "
                      "   OR (from_account = ? AND to_account = ?) "
                      "ORDER BY create_time DESC LIMIT ? OFFSET ?";

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        setError(mysql_error(conn));
        db_.releaseConnection(conn);
        return result;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        setError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return result;
    }

    MYSQL_BIND params[6];
    memset(params, 0, sizeof(params));
    std::string limitStr = std::to_string(limit);
    std::string offsetStr = std::to_string(offset);
    bindParam(&params[0], myAccount);
    bindParam(&params[1], peerAccount);
    bindParam(&params[2], peerAccount);
    bindParam(&params[3], myAccount);
    bindParam(&params[4], limitStr);
    bindParam(&params[5], offsetStr);

    if (mysql_stmt_bind_param(stmt, params) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        setError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return result;
    }

    // 绑定结果
    int msgId, type;
    long sendTime;
    char fromAccount[64] = {0}, toAccount[64] = {0};
    char fromUsername[64] = {0}, fromAvatar[256] = {0}, content[1024] = {0};
    MYSQL_BIND results[8];
    memset(results, 0, sizeof(results));
    results[0].buffer_type = MYSQL_TYPE_LONG; results[0].buffer = &msgId;
    results[1].buffer_type = MYSQL_TYPE_STRING; results[1].buffer = fromAccount; results[1].buffer_length = sizeof(fromAccount);
    results[2].buffer_type = MYSQL_TYPE_STRING; results[2].buffer = fromUsername; results[2].buffer_length = sizeof(fromUsername);
    results[3].buffer_type = MYSQL_TYPE_STRING; results[3].buffer = fromAvatar; results[3].buffer_length = sizeof(fromAvatar);
    results[4].buffer_type = MYSQL_TYPE_STRING; results[4].buffer = toAccount; results[4].buffer_length = sizeof(toAccount);
    results[5].buffer_type = MYSQL_TYPE_STRING; results[5].buffer = content; results[5].buffer_length = sizeof(content);
    results[6].buffer_type = MYSQL_TYPE_LONG; results[6].buffer = &sendTime;
    results[7].buffer_type = MYSQL_TYPE_LONG; results[7].buffer = &type;

    if (mysql_stmt_bind_result(stmt, results) != 0 ||
        mysql_stmt_store_result(stmt) != 0) {
        setError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return result;
    }

    while (mysql_stmt_fetch(stmt) == 0) {
        StoredMessage msg;
        msg.msgId = msgId;
        msg.fromUserId = SessionManager::instance().getUserIdByAccount(fromAccount);
        if (msg.fromUserId <= 0 && std::all_of(fromAccount, fromAccount+strlen(fromAccount), ::isdigit)) {
            msg.fromUserId = std::stoi(fromAccount);
        }
        msg.fromUsername = fromUsername;
        msg.fromAvatar = fromAvatar;
        msg.toId = SessionManager::instance().getUserIdByAccount(toAccount);
        if (msg.toId <= 0 && std::all_of(toAccount, toAccount+strlen(toAccount), ::isdigit)) {
            msg.toId = std::stoi(toAccount);
        }
        msg.content = content;
        msg.sendTime = sendTime;
        msg.type = type;
        result.push_back(msg);
    }

    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    LOG_INFO("[MessageStore] loadHistoryPaginated(single) count=" + std::to_string(result.size()));
    return result;
}

//历史消息（群聊，使用 groupId）修复 from 为 -1 的问题 
std::vector<StoredMessage> MessageStore::loadHistoryPaginated(int groupId,
                                                              int limit, int offset) {
    std::vector<StoredMessage> result;
    MYSQL *conn = db_.getConnection();
    if (!conn) {
        setError("No DB connection");
        return result;
    }

    const char *sql = "SELECT id, from_account, from_username, avatar_url, group_id, "
                      "content, UNIX_TIMESTAMP(create_time), 0 as type "
                      "FROM chat_group_messages WHERE group_id = ? "
                      "ORDER BY create_time DESC LIMIT ? OFFSET ?";
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        setError(mysql_error(conn));
        db_.releaseConnection(conn);
        return result;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        setError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return result;
    }

    MYSQL_BIND params[3];
    memset(params, 0, sizeof(params));
    std::string groupIdStr = std::to_string(groupId);
    std::string limitStr = std::to_string(limit);
    std::string offsetStr = std::to_string(offset);
    bindParam(&params[0], groupIdStr);
    bindParam(&params[1], limitStr);
    bindParam(&params[2], offsetStr);

    if (mysql_stmt_bind_param(stmt, params) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        setError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return result;
    }

    // 绑定结果
    int msgId, type;
    long sendTime;
    char fromAccount[64] = {0}, fromUsername[64] = {0}, fromAvatar[256] = {0}, content[1024] = {0};
    int groupIdOut = 0;
    MYSQL_BIND results[8];
    memset(results, 0, sizeof(results));
    results[0].buffer_type = MYSQL_TYPE_LONG; results[0].buffer = &msgId;
    results[1].buffer_type = MYSQL_TYPE_STRING; results[1].buffer = fromAccount; results[1].buffer_length = sizeof(fromAccount);
    results[2].buffer_type = MYSQL_TYPE_STRING; results[2].buffer = fromUsername; results[2].buffer_length = sizeof(fromUsername);
    results[3].buffer_type = MYSQL_TYPE_STRING; results[3].buffer = fromAvatar; results[3].buffer_length = sizeof(fromAvatar);
    results[4].buffer_type = MYSQL_TYPE_LONG; results[4].buffer = &groupIdOut;
    results[5].buffer_type = MYSQL_TYPE_STRING; results[5].buffer = content; results[5].buffer_length = sizeof(content);
    results[6].buffer_type = MYSQL_TYPE_LONG; results[6].buffer = &sendTime;
    results[7].buffer_type = MYSQL_TYPE_LONG; results[7].buffer = &type;

    if (mysql_stmt_bind_result(stmt, results) != 0 ||
        mysql_stmt_store_result(stmt) != 0) {
        setError(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return result;
    }

    while (mysql_stmt_fetch(stmt) == 0) {
        StoredMessage msg;
        msg.msgId = msgId;
        // FIXED: 将 fromAccount 转换为 userId，支持账号和兼容纯数字
        int fromId = SessionManager::instance().getUserIdByAccount(fromAccount);
        if (fromId <= 0 && std::all_of(fromAccount, fromAccount+strlen(fromAccount), ::isdigit)) {
            fromId = std::stoi(fromAccount);
        }
        msg.fromUserId = fromId;
        msg.fromUsername = fromUsername;
        msg.fromAvatar = fromAvatar;
        msg.toId = groupIdOut;
        msg.content = content;
        msg.sendTime = sendTime;
        msg.type = type;
        result.push_back(msg);
    }

    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    LOG_INFO("[MessageStore] loadHistoryPaginated(group) count=" + std::to_string(result.size()));
    return result;
}

//  会话摘要
void MessageStore::updateConversationSummary(int userId, const std::string &convType,
                                             const std::string &convId, const std::string &lastMsg,
                                             time_t lastTime, bool incrementUnread) {
    std::string key = "user:conv:" + std::to_string(userId);
    std::string prefix = convType + ":" + convId;

    if (incrementUnread) {
        redis_.hincrby(key, prefix + ":unread", 1);
    }
    redis_.hset(key, prefix + ":lastMsg", lastMsg);
    redis_.hset(key, prefix + ":lastTime", std::to_string(lastTime));

    LOG_INFO("[MessageStore] updateConversationSummary user=" + std::to_string(userId) +
             " conv=" + prefix + " incr=" + (incrementUnread ? "1" : "0"));
}

// 会话列表重建 
void MessageStore::rebuildConversations(int userId, bool force) {
    std::string convKey = "user:conv:" + std::to_string(userId);
    if (!force && redis_.exists(convKey)) {
        LOG_INFO("[MessageStore] rebuildConversations: cache exists for user " +
                 std::to_string(userId) + ", skip");
        return;
    }

    MYSQL *conn = db_.getConnection();
    if (!conn) {
        LOG_ERROR("[MessageStore] rebuildConversations: 无法获取数据库连接");
        return;
    }

    // 1. 私聊会话（使用账号）
    std::string sql = R"(
        SELECT 
            IF(from_account = ?, to_account, from_account) AS peer_account,
            content,
            UNIX_TIMESTAMP(create_time) AS msg_time
        FROM chat_single_messages
        WHERE from_account = ? OR to_account = ?
        ORDER BY create_time DESC
        LIMIT 200
    )";

    std::string myAccount = SessionManager::instance().getAccountByUserId(userId);
    if (myAccount.empty()) {
        LOG_ERROR("[MessageStore] rebuildConversations: cannot get account for userId " + std::to_string(userId));
        db_.releaseConnection(conn);
        return;
    }

    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) {
        LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_init failed");
        db_.releaseConnection(conn);
        return;
    }

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length()) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: prepare failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    MYSQL_BIND params[3];
    memset(params, 0, sizeof(params));
    bindParam(&params[0], myAccount);
    bindParam(&params[1], myAccount);
    bindParam(&params[2], myAccount);
    if (mysql_stmt_bind_param(stmt, params) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: bind param failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: execute failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    MYSQL_BIND result[3];
    memset(result, 0, sizeof(result));
    char peerAccountBuf[64] = {0}, contentBuf[1024] = {0};
    long msgTime;
    result[0].buffer_type = MYSQL_TYPE_STRING; result[0].buffer = peerAccountBuf; result[0].buffer_length = sizeof(peerAccountBuf);
    result[1].buffer_type = MYSQL_TYPE_STRING; result[1].buffer = contentBuf; result[1].buffer_length = sizeof(contentBuf);
    result[2].buffer_type = MYSQL_TYPE_LONG; result[2].buffer = &msgTime;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: bind result failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: store result failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    std::unordered_map<std::string, std::pair<std::string, time_t>> singleMap;
    while (mysql_stmt_fetch(stmt) == 0) {
        std::string peerAccount(peerAccountBuf);
        std::string lastMsg(contentBuf);
        auto it = singleMap.find(peerAccount);
        if (it == singleMap.end() || msgTime > it->second.second) {
            singleMap[peerAccount] = {lastMsg, msgTime};
        }
    }
    mysql_stmt_close(stmt);

    // 2. 群聊会话
    std::vector<int> groupIds;
    sql = "SELECT group_id FROM group_members WHERE user_id = ?";
    stmt = mysql_stmt_init(conn);
    if (!stmt) {
        LOG_ERROR("[MessageStore] rebuildConversations: init group members failed");
        db_.releaseConnection(conn);
        return;
    }

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length()) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: prepare group members failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    MYSQL_BIND param;
    memset(&param, 0, sizeof(param));
    std::string userIdStr = std::to_string(userId);
    bindParam(&param, userIdStr);
    if (mysql_stmt_bind_param(stmt, &param) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: bind param group members failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: execute group members failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    MYSQL_BIND result_gid;
    int gid = 0;
    memset(&result_gid, 0, sizeof(result_gid));
    result_gid.buffer_type = MYSQL_TYPE_LONG;
    result_gid.buffer = &gid;
    if (mysql_stmt_bind_result(stmt, &result_gid) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: bind result group members failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        LOG_ERROR("[MessageStore] rebuildConversations: store result group members failed");
        mysql_stmt_close(stmt);
        db_.releaseConnection(conn);
        return;
    }

    while (mysql_stmt_fetch(stmt) == 0) {
        groupIds.push_back(gid);
    }
    mysql_stmt_close(stmt);

    std::unordered_map<std::string, std::pair<std::string, time_t>> groupMap;
    for (int gid_val : groupIds) {
        sql = "SELECT content, UNIX_TIMESTAMP(create_time) FROM chat_group_messages "
              "WHERE group_id = ? ORDER BY create_time DESC LIMIT 1";
        stmt = mysql_stmt_init(conn);
        if (!stmt) continue;

        if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length()) != 0) {
            mysql_stmt_close(stmt);
            continue;
        }

        MYSQL_BIND param_gid;
        memset(&param_gid, 0, sizeof(param_gid));
        param_gid.buffer_type = MYSQL_TYPE_LONG;
        param_gid.buffer = &gid_val;
        if (mysql_stmt_bind_param(stmt, &param_gid) != 0) {
            mysql_stmt_close(stmt);
            continue;
        }

        if (mysql_stmt_execute(stmt) != 0) {
            mysql_stmt_close(stmt);
            continue;
        }

        MYSQL_BIND result_msg[2];
        char content[1024] = {0};
        long lastTime = 0;
        memset(result_msg, 0, sizeof(result_msg));
        result_msg[0].buffer_type = MYSQL_TYPE_STRING;
        result_msg[0].buffer = content;
        result_msg[0].buffer_length = sizeof(content);
        result_msg[1].buffer_type = MYSQL_TYPE_LONG;
        result_msg[1].buffer = &lastTime;
        if (mysql_stmt_bind_result(stmt, result_msg) != 0) {
            mysql_stmt_close(stmt);
            continue;
        }

        if (mysql_stmt_fetch(stmt) == 0) {
            std::string field = "group:" + std::to_string(gid_val);
            groupMap[field] = {std::string(content), lastTime};
        }
        mysql_stmt_close(stmt);
    }

    db_.releaseConnection(conn);

    // 3. 写入 Redis
    for (const auto &[peerAccount, data] : singleMap) {
        std::string prefix = "single:" + peerAccount;
        redis_.hset(convKey, prefix + ":lastMsg", data.first);
        redis_.hset(convKey, prefix + ":lastTime", std::to_string(data.second));
        redis_.hset(convKey, prefix + ":unread", "0");
    }
    for (const auto &[field, data] : groupMap) {
        redis_.hset(convKey, field + ":lastMsg", data.first);
        redis_.hset(convKey, field + ":lastTime", std::to_string(data.second));
        redis_.hset(convKey, field + ":unread", "0");
    }

    redis_.expire(convKey, 7 * 24 * 3600);
    LOG_INFO("[MessageStore] rebuildConversations: user=" + std::to_string(userId) +
             " private=" + std::to_string(singleMap.size()) +
             " group=" + std::to_string(groupMap.size()));
}

void MessageStore::asyncRebuildConversations(int userId, bool force) {
    AeroQueue::instance().post([this, userId, force]() { rebuildConversations(userId, force); });
}

bool MessageStore::setError(const std::string &err) {
    std::lock_guard<std::mutex> lock(mutex_);
    last_error_ = err;
    LOG_ERROR("[MessageStore] Error: " + err);
    return false;
}

std::string MessageStore::getLastError() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_error_;
}