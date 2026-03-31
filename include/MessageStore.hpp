/*
MessageStore类:
为什么要有这个类？
用户1在用户2进行聊天的时候，一定会有聊天记录的存在，我们如何确保用户退出登录后或者重新登录后历史聊天记录依旧存在呢？
那就不得不引入MySQL来做数据持久化即消息持久化
Message就是AeroChat中负责消息持久化以及离线消息缓存还有历史消息加载的核心类,与DBManager(操作数据库)以及
RedisClient(缓存离线消息)协作,来保证数据持久化
*/
#ifndef MESSAGESTORE_HPP
#define MESSAGESTORE_HPP

#include "DBManager.hpp"
#include "RedisClient.hpp"
#include "GroupManager.hpp"
#include <vector>
#include <string>
#include <mutex>
#include <unordered_map>

#define INBOX_KEY_PREFIX "inbox:"

struct StoredMessage {
    int msgId;
    int fromUserId;
    std::string fromUsername;
    std::string fromAvatar;
    int toId;          // 群聊时为 groupId，私聊时为对方 userId
    std::string content;
    long sendTime;
    int type;          // 0: group, 1: single
};

class MessageStore {
public:
    static MessageStore& instance();

    // 保存消息，返回 msgId
    int saveMessage(int fromUserId, const std::string& fromUsername, const std::string& fromAvatar,
                    int toId, bool isGroup, const std::string& content);

    // 离线消息
    void pushToInbox(int msgId, const std::vector<int>& targetUserIds);
    std::vector<int> pullOfflineMsgIds(int userId);
    std::vector<StoredMessage> getMessages(const std::vector<int>& msgIds);

    // 历史消息
    std::vector<StoredMessage> loadHistory(int userId1, int userId2, bool isGroup, int limit);

    // 会话摘要
    void updateConversationSummary(int userId, const std::string& convType, const std::string& convId,
                                   const std::string& lastMsg, time_t lastTime, bool incrementUnread);

    // ========== 新增：会话列表重建（支持强制/按需） ==========
    void rebuildConversations(int userId, bool force = false);
    void asyncRebuildConversations(int userId, bool force = false);

    std::string getLastError() const;

private:
    MessageStore();
    bool setError(const std::string& err);
    void bindParam(MYSQL_BIND* bind, const std::string& value);

    DBManager& db_;
    RedisClient& redis_;
    GroupManager& group_;
    mutable std::mutex mutex_;
    std::string last_error_;
};

#endif