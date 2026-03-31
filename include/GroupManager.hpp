#ifndef GROUPMANAGER_HPP
#define GROUPMANAGER_HPP

#include "RedisClient.hpp"
#include "DBManager.hpp"
#include <vector>
#include <string>

#define GROUP_KEY_PREFIX "group:"

class GroupManager {
public:
    static GroupManager& instance();
    std::vector<int> getGroupMembers(int groupId);
    bool isInGroup(int groupId, int userId);
    void initDefaultGroup();
    bool addUserToGroup(int groupId, int userId);

    // 新增：从数据库加载所有群组成员到 Redis（用于 Redis 启动后恢复）
    void loadAllGroupsFromDB();

private:
    GroupManager() : redis_(RedisClient::instance()) {}
    RedisClient& redis_;
};

#endif