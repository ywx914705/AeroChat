#ifndef MESSAGESTORE_HPP
#define MESSAGESTORE_HPP

#include "DBManager.hpp"
#include "RedisClient.hpp"
#include "GroupManager.hpp"
#include <vector>
#include <string>
#include <mutex>

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

    // 群聊保存消息（原有）
    int saveMessage(int fromUserId, const std::string& fromUsername,
                    const std::string& fromAvatar, int toId,
                    bool isGroup, const std::string& content);

    // 私聊保存消息（新增：使用账号）
    int saveMessage(int fromUserId, const std::string& fromUsername,
                    const std::string& fromAvatar, const std::string& toAccount,
                    const std::string& content);

    // 离线消息
    void pushToInbox(int msgId, const std::vector<int>& targetUserIds);
    std::vector<int> pullOfflineMsgIds(int userId);
    std::vector<StoredMessage> getMessages(const std::vector<int>& msgIds);

    // 历史消息（群聊，使用 groupId）
    std::vector<StoredMessage> loadHistoryPaginated(int groupId, int limit, int offset);

    // 历史消息（私聊，使用账号）
    std::vector<StoredMessage> loadHistoryPaginated(const std::string& myAccount,
                                                    const std::string& peerAccount,
                                                    int limit, int offset);

    // 会话摘要
    void updateConversationSummary(int userId, const std::string& convType,
                                   const std::string& convId,
                                   const std::string& lastMsg,
                                   time_t lastTime, bool incrementUnread);

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

#endif // MESSAGESTORE_HPP