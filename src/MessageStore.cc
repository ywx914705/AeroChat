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

int MessageStore::saveMessage(int fromUserId, const std::string &fromUsername,
                              const std::string &fromAvatar, int toId,
                              bool isGroup, const std::string &content) {
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

  const char *sql;
  MYSQL_BIND params[5];
  memset(params, 0, sizeof(params));
  int paramCount = 0;

  std::string fromUserIdStr = std::to_string(fromUserId);
  std::string toIdStr = std::to_string(toId);

  if (isGroup) {
    sql = "INSERT INTO chat_group_messages (from_account, from_username, "
          "avatar_url, group_id, content, create_time) VALUES (?, ?, ?, ?, ?, "
          "NOW())";
    bindParam(&params[paramCount++], fromUserIdStr);
    bindParam(&params[paramCount++], fromUsername);
    bindParam(&params[paramCount++], fromAvatar);
    bindParam(&params[paramCount++], toIdStr);
    bindParam(&params[paramCount++], content);
  } else {
    sql = "INSERT INTO chat_single_messages (from_account, from_username, "
          "avatar_url, to_account, content, create_time) VALUES (?, ?, ?, ?, "
          "?, NOW())";
    bindParam(&params[paramCount++], fromUserIdStr);
    bindParam(&params[paramCount++], fromUsername);
    bindParam(&params[paramCount++], fromAvatar);
    bindParam(&params[paramCount++], toIdStr);
    bindParam(&params[paramCount++], content);
  }

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

  LOG_INFO("[MessageStore] saveMessage success, msgId=" +
           std::to_string(msgId) + " from=" + fromUserIdStr + " to=" + toIdStr +
           " isGroup=" + (isGroup ? "true" : "false"));

  // ========== 更新会话摘要 ==========
  time_t now = time(nullptr);
  std::string convType = isGroup ? "group" : "single";
  std::string convId = isGroup ? std::to_string(toId) : std::to_string(toId);

  updateConversationSummary(fromUserId, convType, convId, content, now, false);

  if (isGroup) {
    auto members = group_.getGroupMembers(toId);
    for (int uid : members) {
      if (uid != fromUserId) {
        updateConversationSummary(uid, convType, convId, content, now, true);
      }
    }
  } else {
    updateConversationSummary(toId, convType, convId, content, now, true);
  }

  return msgId;
}

void MessageStore::pushToInbox(int msgId,
                               const std::vector<int> &targetUserIds) {
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

std::vector<StoredMessage>
MessageStore::getMessages(const std::vector<int> &msgIds) {
  std::vector<StoredMessage> result;
  if (msgIds.empty())
    return result;

  std::string ids;
  for (size_t i = 0; i < msgIds.size(); ++i) {
    if (i > 0)
      ids += ",";
    ids += std::to_string(msgIds[i]);
  }

  std::string sql = "SELECT id, from_account, from_username, avatar_url, "
                    "group_id, content, UNIX_TIMESTAMP(create_time), 0 as type "
                    "FROM chat_group_messages WHERE id IN (" +
                    ids +
                    ")"
                    " UNION ALL "
                    "SELECT id, from_account, from_username, avatar_url, "
                    "to_account, content, UNIX_TIMESTAMP(create_time), 1 as "
                    "type FROM chat_single_messages WHERE id IN (" +
                    ids + ")";

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
    msg.fromUserId = std::stoi(row[1]);
    msg.fromUsername = row[2] ? row[2] : "";
    msg.fromAvatar = row[3] ? row[3] : "";
    msg.toId = (row[4] ? std::stoi(row[4]) : 0);
    msg.content = row[5] ? row[5] : "";
    msg.sendTime = std::stol(row[6]);
    msg.type = std::stoi(row[7]);
    result.push_back(msg);
  }
  mysql_free_result(res);
  db_.releaseConnection(conn);
  return result;
}

std::vector<StoredMessage> MessageStore::loadHistory(int userId1, int userId2,
                                                     bool isGroup, int limit) {
  std::vector<StoredMessage> result;
  MYSQL *conn = db_.getConnection();
  if (!conn) {
    setError("No DB connection");
    return result;
  }

  MYSQL_STMT *stmt = mysql_stmt_init(conn);
  if (!stmt) {
    setError(mysql_error(conn));
    db_.releaseConnection(conn);
    return result;
  }

  const char *sql;
  MYSQL_BIND params[5];
  memset(params, 0, sizeof(params));
  int paramCount = 0;

  std::string limitStr = std::to_string(limit);

  if (isGroup) {
    sql = "SELECT id, from_account, from_username, avatar_url, group_id, "
          "content, UNIX_TIMESTAMP(create_time), 0 as type "
          "FROM chat_group_messages WHERE group_id = ? ORDER BY create_time "
          "DESC LIMIT ?";
    std::string groupIdStr = std::to_string(userId2);
    bindParam(&params[paramCount++], groupIdStr);
    bindParam(&params[paramCount++], limitStr);
    LOG_INFO("[MessageStore] loadHistory group: groupIdStr=" + groupIdStr +
             ", limit=" + limitStr);
  } else {
    sql = "SELECT id, from_account, from_username, avatar_url, to_account, "
          "content, UNIX_TIMESTAMP(create_time), 1 as type "
          "FROM chat_single_messages "
          "WHERE (from_account = ? AND to_account = ?) OR (from_account = ? "
          "AND to_account = ?) "
          "ORDER BY create_time DESC LIMIT ?";
    std::string user1Str = std::to_string(userId1);
    std::string user2Str = std::to_string(userId2);
    bindParam(&params[paramCount++], user1Str);
    bindParam(&params[paramCount++], user2Str);
    bindParam(&params[paramCount++], user2Str);
    bindParam(&params[paramCount++], user1Str);
    bindParam(&params[paramCount++], limitStr);
    LOG_INFO("[MessageStore] loadHistory private: user1=" + user1Str +
             ", user2=" + user2Str + ", limit=" + limitStr);
  }

  if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0 ||
      mysql_stmt_bind_param(stmt, params) != 0 ||
      mysql_stmt_execute(stmt) != 0) {
    setError(mysql_stmt_error(stmt));
    LOG_ERROR("[MessageStore] loadHistory execute failed: " + last_error_);
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return result;
  }

  char fromAccount[32] = {0}, fromUsername[64] = {0}, fromAvatar[256] = {0},
       toAccount[32] = {0}, content[1024] = {0};
  int msgId, type;
  long sendTime;
  MYSQL_BIND results[8];
  memset(results, 0, sizeof(results));
  results[0].buffer_type = MYSQL_TYPE_LONG;
  results[0].buffer = &msgId;
  results[1].buffer_type = MYSQL_TYPE_STRING;
  results[1].buffer = fromAccount;
  results[1].buffer_length = sizeof(fromAccount);
  results[2].buffer_type = MYSQL_TYPE_STRING;
  results[2].buffer = fromUsername;
  results[2].buffer_length = sizeof(fromUsername);
  results[3].buffer_type = MYSQL_TYPE_STRING;
  results[3].buffer = fromAvatar;
  results[3].buffer_length = sizeof(fromAvatar);
  results[4].buffer_type = MYSQL_TYPE_STRING;
  results[4].buffer = toAccount;
  results[4].buffer_length = sizeof(toAccount);
  results[5].buffer_type = MYSQL_TYPE_STRING;
  results[5].buffer = content;
  results[5].buffer_length = sizeof(content);
  results[6].buffer_type = MYSQL_TYPE_LONG;
  results[6].buffer = &sendTime;
  results[7].buffer_type = MYSQL_TYPE_LONG;
  results[7].buffer = &type;

  if (mysql_stmt_bind_result(stmt, results) != 0) {
    setError(mysql_stmt_error(stmt));
    LOG_ERROR("[MessageStore] loadHistory bind result failed: " + last_error_);
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return result;
  }

  if (mysql_stmt_store_result(stmt) != 0) {
    setError(mysql_stmt_error(stmt));
    LOG_ERROR("[MessageStore] loadHistory store result failed: " + last_error_);
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return result;
  }

  int rowCount = 0;
  while (mysql_stmt_fetch(stmt) == 0) {
    rowCount++;
    StoredMessage msg;
    msg.msgId = msgId;
    msg.fromUserId = std::stoi(fromAccount);
    msg.fromUsername = fromUsername;
    msg.fromAvatar = fromAvatar;
    msg.toId = std::stoi(toAccount);
    msg.content = content;
    msg.sendTime = sendTime;
    msg.type = type;
    result.push_back(msg);
  }

  mysql_stmt_close(stmt);
  db_.releaseConnection(conn);
  LOG_INFO("[MessageStore] loadHistory " +
           std::string(isGroup ? "group" : "private") +
           " id=" + std::to_string(isGroup ? userId2 : userId2) +
           " count=" + std::to_string(result.size()) +
           " rows_fetched=" + std::to_string(rowCount));
  return result;
}

// ========== 修改：使用独立字段存储，HINCRBY 原子递增未读数 ==========
void MessageStore::updateConversationSummary(
    int userId, const std::string &convType, const std::string &convId,
    const std::string &lastMsg, time_t lastTime, bool incrementUnread) {
  std::string key = "user:conv:" + std::to_string(userId);
  std::string prefix = convType + ":" + convId;

  if (incrementUnread) {
    redis_.hincrby(key, prefix + ":unread", 1);
  }
  // 覆盖最后消息和时间
  redis_.hset(key, prefix + ":lastMsg", lastMsg);
  redis_.hset(key, prefix + ":lastTime", std::to_string(lastTime));

  LOG_INFO("[MessageStore] updateConversationSummary user=" +
           std::to_string(userId) + " conv=" + prefix +
           " incr=" + (incrementUnread ? "1" : "0"));
}

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

  // ---------- 1. 私聊会话 ----------
  std::string sql = R"(
        SELECT 
            IF(from_account = ?, to_account, from_account) AS peer_id,
            content,
            UNIX_TIMESTAMP(create_time) AS msg_time
        FROM chat_single_messages
        WHERE from_account = ? OR to_account = ?
        ORDER BY create_time DESC
        LIMIT 200
    )";

  std::string userIdStr = std::to_string(userId);
  MYSQL_STMT *stmt = mysql_stmt_init(conn);
  if (!stmt) {
    LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_init 失败: " +
              std::string(mysql_error(conn)));
    db_.releaseConnection(conn);
    return;
  }

  if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length()) != 0) {
    LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_prepare 失败: " +
              std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  MYSQL_BIND params[3];
  memset(params, 0, sizeof(params));
  params[0].buffer_type = MYSQL_TYPE_STRING;
  params[0].buffer = (char *)userIdStr.c_str();
  params[0].buffer_length = userIdStr.length();
  params[1] = params[0];
  params[2] = params[0];
  if (mysql_stmt_bind_param(stmt, params) != 0) {
    LOG_ERROR(
        "[MessageStore] rebuildConversations: mysql_stmt_bind_param 失败: " +
        std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  if (mysql_stmt_execute(stmt) != 0) {
    LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_execute 失败: " +
              std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  MYSQL_BIND result[3];
  memset(result, 0, sizeof(result));
  char peerIdBuf[64] = {0}, contentBuf[1024] = {0};
  long msgTime;
  result[0].buffer_type = MYSQL_TYPE_STRING;
  result[0].buffer = peerIdBuf;
  result[0].buffer_length = sizeof(peerIdBuf);
  result[1].buffer_type = MYSQL_TYPE_STRING;
  result[1].buffer = contentBuf;
  result[1].buffer_length = sizeof(contentBuf);
  result[2].buffer_type = MYSQL_TYPE_LONG;
  result[2].buffer = &msgTime;
  if (mysql_stmt_bind_result(stmt, result) != 0) {
    LOG_ERROR(
        "[MessageStore] rebuildConversations: mysql_stmt_bind_result 失败: " +
        std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  if (mysql_stmt_store_result(stmt) != 0) {
    LOG_ERROR(
        "[MessageStore] rebuildConversations: mysql_stmt_store_result 失败: " +
        std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  std::unordered_map<std::string, std::pair<std::string, time_t>> singleMap;
  while (mysql_stmt_fetch(stmt) == 0) {
    std::string peerId(peerIdBuf);
    std::string lastMsg(contentBuf);
    auto it = singleMap.find(peerId);
    if (it == singleMap.end() || msgTime > it->second.second) {
      singleMap[peerId] = {lastMsg, msgTime};
    }
  }

  // 释放 stmt，后面会重新创建用于群聊查询
  mysql_stmt_close(stmt);

  // ---------- 2. 群聊会话 ----------
  std::vector<int> groupIds;
  sql = "SELECT group_id FROM group_members WHERE user_id = ?";
  stmt = mysql_stmt_init(conn);
  if (!stmt) {
    LOG_ERROR(
        "[MessageStore] rebuildConversations: mysql_stmt_init (群成员) 失败: " +
        std::string(mysql_error(conn)));
    db_.releaseConnection(conn);
    return;
  }

  if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length()) != 0) {
    LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_prepare "
              "(群成员) 失败: " +
              std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  MYSQL_BIND param;
  memset(&param, 0, sizeof(param));
  param.buffer_type = MYSQL_TYPE_STRING;
  param.buffer = (char *)userIdStr.c_str();
  param.buffer_length = userIdStr.length();
  if (mysql_stmt_bind_param(stmt, &param) != 0) {
    LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_bind_param "
              "(群成员) 失败: " +
              std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  if (mysql_stmt_execute(stmt) != 0) {
    LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_execute "
              "(群成员) 失败: " +
              std::string(mysql_stmt_error(stmt)));
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
    LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_bind_result "
              "(群成员) 失败: " +
              std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  if (mysql_stmt_store_result(stmt) != 0) {
    LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_store_result "
              "(群成员) 失败: " +
              std::string(mysql_stmt_error(stmt)));
    mysql_stmt_close(stmt);
    db_.releaseConnection(conn);
    return;
  }

  while (mysql_stmt_fetch(stmt) == 0) {
    groupIds.push_back(gid);
  }
  mysql_stmt_close(stmt);

  // 群聊最新消息
  std::unordered_map<std::string, std::pair<std::string, time_t>> groupMap;
  for (int gid_val : groupIds) {
    sql =
        "SELECT content, UNIX_TIMESTAMP(create_time) FROM chat_group_messages "
        "WHERE group_id = ? ORDER BY create_time DESC LIMIT 1";
    stmt = mysql_stmt_init(conn);
    if (!stmt) {
      LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_init (群消息) "
                "失败: " +
                std::string(mysql_error(conn)));
      continue;
    }

    if (mysql_stmt_prepare(stmt, sql.c_str(), sql.length()) != 0) {
      LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_prepare "
                "(群消息) 失败: " +
                std::string(mysql_stmt_error(stmt)));
      mysql_stmt_close(stmt);
      continue;
    }

    MYSQL_BIND param_gid;
    memset(&param_gid, 0, sizeof(param_gid));
    param_gid.buffer_type = MYSQL_TYPE_LONG;
    param_gid.buffer = &gid_val;
    if (mysql_stmt_bind_param(stmt, &param_gid) != 0) {
      LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_bind_param "
                "(群消息) 失败: " +
                std::string(mysql_stmt_error(stmt)));
      mysql_stmt_close(stmt);
      continue;
    }

    if (mysql_stmt_execute(stmt) != 0) {
      LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_execute "
                "(群消息) 失败: " +
                std::string(mysql_stmt_error(stmt)));
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
      LOG_ERROR("[MessageStore] rebuildConversations: mysql_stmt_bind_result "
                "(群消息) 失败: " +
                std::string(mysql_stmt_error(stmt)));
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

  // 3. 写入独立字段
  for (const auto &[peerId, data] : singleMap) {
    std::string prefix = "single:" + peerId;
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
  LOG_INFO(
      "[MessageStore] rebuildConversations: user=" + std::to_string(userId) +
      " updated private=" + std::to_string(singleMap.size()) +
      " group=" + std::to_string(groupMap.size()));
}
void MessageStore::asyncRebuildConversations(int userId, bool force) {
  AeroQueue::instance().post(
      [this, userId, force]() { rebuildConversations(userId, force); });
}

bool MessageStore::setError(const std::string &err) {
  std::lock_guard<std::mutex> lock(mutex_);
  last_error_ = err;
  LOG_ERROR("[MessageStore] Error: " + err);
  return false;
}