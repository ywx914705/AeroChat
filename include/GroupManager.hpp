#ifndef GROUPMANAGER_HPP
#define GROUPMANAGER_HPP

#include "RedisClient.hpp"
#include "DBManager.hpp"
#include <vector>
#include <string>
#include <unordered_map>

#define GROUP_KEY_PREFIX "group:"

struct GroupInfo {
    int id;
    std::string name;
    std::string description;
    std::string avatar;
    int memberCount;      // 当前成员数（缓存）
    int maxMembers;       // 最大成员数（例如3000）
};

class GroupManager {
public:
    static GroupManager& instance();

    // 初始化预设群组（8个）
    void initPresetGroups();

    // 获取所有群组列表（用于前端展示）
    std::vector<GroupInfo> getAllGroups() const;

    // 获取单个群组信息
    GroupInfo getGroupInfo(int groupId) const;

    // 获取群组成员（用户ID列表）—— 全量，慎用
    std::vector<int> getGroupMembers(int groupId);

    // 分页获取群组成员（推荐）
    std::vector<int> getGroupMembersPaginated(int groupId, int offset, int limit);

    // 判断用户是否在群组中
    bool isInGroup(int groupId, int userId);

    // 用户加入群组（maxUserGroups：用户最多能加入的群组数量，默认3）
    bool addUserToGroup(int groupId, int userId, int maxUserGroups = 3);

    // 用户退出群组
    bool removeUserFromGroup(int groupId, int userId);

    // 获取用户加入的所有群组ID列表
    std::vector<int> getUserGroups(int userId) const;

    // 获取用户加入的群组数量
    int getUserGroupCount(int userId) const;

    // 加载所有群组成员到Redis（启动时调用）
    void loadAllGroupsFromDB();

private:
    GroupManager();
    RedisClient& redis_;
    std::vector<GroupInfo> presetGroups_;  // 预设群组列表
};

#endif // GROUPMANAGER_HPP