#include "GroupManager.hpp"
#include "DBManager.hpp"
#include "Log.hpp"

GroupManager::GroupManager() : redis_(RedisClient::instance()) {}

GroupManager& GroupManager::instance() {
    static GroupManager instance;
    return instance;
}

void GroupManager::initPresetGroups() {
    // 预设8个群组（id从1到8）
    presetGroups_ = {
        {1, "💻 编程技术交流", "讨论编程语言、框架、算法等", "https://cdn-icons-png.flaticon.com/512/1998/1998592.png", 0, 3000},
        {2, "🍵 生活杂谈", "分享日常生活、趣事、心情", "https://cdn-icons-png.flaticon.com/512/3075/3075977.png", 0, 3000},
        {3, "🎮 游戏娱乐", "游戏开黑、攻略、赛事讨论", "https://cdn-icons-png.flaticon.com/512/3659/3659788.png", 0, 3000},
        {4, "📚 学习考试", "考研、考证、学习资料分享", "https://cdn-icons-png.flaticon.com/512/3135/3135715.png", 0, 3000},
        {5, "🎵 音乐影视", "推荐好歌、电影、剧集", "https://cdn-icons-png.flaticon.com/512/1384/1384065.png", 0, 3000},
        {6, "🏀 体育运动", "篮球、足球、健身交流", "https://cdn-icons-png.flaticon.com/512/3096/3096142.png", 0, 3000},
        {7, "🚀 科技前沿", "AI、区块链、数码产品", "https://cdn-icons-png.flaticon.com/512/1055/1055687.png", 0, 3000},
        {8, "🌈 情感树洞", "匿名倾诉、情感互助", "https://cdn-icons-png.flaticon.com/512/2620/2620762.png", 0, 3000}
    };

    // 将预设群组信息写入Redis（缓存元数据）
    for (const auto& group : presetGroups_) {
        std::string key = "group:info:" + std::to_string(group.id);
        redis_.hset(key, "name", group.name);
        redis_.hset(key, "description", group.description);
        redis_.hset(key, "avatar", group.avatar);
        redis_.hset(key, "maxMembers", std::to_string(group.maxMembers));
        // 成员数从Redis的Set中获取，不在这里设置
    }
    LOG_INFO("[GroupManager] 初始化8个预设群组");
}

std::vector<GroupInfo> GroupManager::getAllGroups() const {
    std::vector<GroupInfo> result;
    for (auto& group : presetGroups_) {
        // 实时获取成员数（从Redis的Set中获取）
        std::string key = GROUP_KEY_PREFIX + std::to_string(group.id);
        long long count = redis_.scard(key);
        GroupInfo info = group;
        info.memberCount = static_cast<int>(count);
        result.push_back(info);
    }
    return result;
}

GroupInfo GroupManager::getGroupInfo(int groupId) const {
    for (const auto& g : presetGroups_) {
        if (g.id == groupId) {
            GroupInfo info = g;
            std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
            info.memberCount = static_cast<int>(redis_.scard(key));
            return info;
        }
    }
    return GroupInfo{};
}

std::vector<int> GroupManager::getGroupMembers(int groupId) {
    std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
    auto membersStr = redis_.smembers(key);
    std::vector<int> result;
    for (const auto& m : membersStr) {
        result.push_back(std::stoi(m));
    }
    return result;
}

// 分页获取群组成员，使用 SSCAN 避免阻塞
std::vector<int> GroupManager::getGroupMembersPaginated(int groupId, int offset, int limit) {
    std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
    std::vector<int> result;
    // 使用 SSCAN 迭代，跳过 offset 个元素，取 limit 个
    // 注意：Redis 的 SSCAN 不保证顺序，但这里我们不需要顺序
    size_t cursor = 0;
    int skipped = 0;
    do {
        std::vector<std::string> batch;
        cursor = redis_.sscan(key, cursor, batch, 100);  // 每次取100个
        for (const auto& member : batch) {
            if (skipped < offset) {
                skipped++;
                continue;
            }
            result.push_back(std::stoi(member));
            if (result.size() >= static_cast<size_t>(limit)) {
                return result;
            }
        }
    } while (cursor != 0);
    return result;
}

bool GroupManager::isInGroup(int groupId, int userId) {
    std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
    return redis_.sismember(key, std::to_string(userId));
}

bool GroupManager::addUserToGroup(int groupId, int userId, int maxUserGroups) {
    // 1. 检查用户是否已在群组中
    if (isInGroup(groupId, userId)) {
        LOG_WARN("[GroupManager] 用户 " + std::to_string(userId) + " 已在群组 " + std::to_string(groupId) + " 中");
        return false;
    }

    // 2. 检查用户已加入的群组数量是否达到上限
    int currentCount = getUserGroupCount(userId);
    if (currentCount >= maxUserGroups) {
        LOG_WARN("[GroupManager] 用户 " + std::to_string(userId) + " 已加入 " + std::to_string(currentCount) + " 个群组，已达上限");
        return false;
    }

    // 3. 检查群组是否已满
    std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
    long long currentMembers = redis_.scard(key);
    int maxMembers = 3000; // 可以从预设中获取
    if (currentMembers >= maxMembers) {
        LOG_WARN("[GroupManager] 群组 " + std::to_string(groupId) + " 已满 (" + std::to_string(currentMembers) + "/" + std::to_string(maxMembers) + ")");
        return false;
    }

    // 4. 写入数据库（记录成员关系，用于持久化）
    bool dbOk = DBManager::getInstance().addGroupMember(groupId, userId);
    if (!dbOk) {
        LOG_ERROR("[GroupManager] 数据库添加群组成员失败");
        return false;
    }

    // 5. 写入Redis
    long long ret = redis_.sadd(key, std::to_string(userId));
    if (ret > 0) {
        LOG_INFO("[GroupManager] 用户 " + std::to_string(userId) + " 加入群组 " + std::to_string(groupId));
        return true;
    } else {
        LOG_ERROR("[GroupManager] Redis添加成员失败");
        return false;
    }
}

bool GroupManager::removeUserFromGroup(int groupId, int userId) {
    if (!isInGroup(groupId, userId)) {
        return false;
    }

    // 从数据库删除
    bool dbOk = DBManager::getInstance().removeGroupMember(groupId, userId);
    if (!dbOk) {
        LOG_ERROR("[GroupManager] 数据库删除群组成员失败");
        return false;
    }

    // 从Redis删除
    std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
    long long ret = redis_.srem(key, std::to_string(userId));
    if (ret > 0) {
        LOG_INFO("[GroupManager] 用户 " + std::to_string(userId) + " 退出群组 " + std::to_string(groupId));
        return true;
    }
    return false;
}

std::vector<int> GroupManager::getUserGroups(int userId) const {
    return DBManager::getInstance().getUserGroupsFromDB(userId);
}

int GroupManager::getUserGroupCount(int userId) const {
    auto groups = getUserGroups(userId);
    return static_cast<int>(groups.size());
}

void GroupManager::loadAllGroupsFromDB() {
    // 从数据库加载所有群组成员到Redis（用于重启后恢复）
    for (int groupId = 1; groupId <= 8; ++groupId) {
        std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
        if (redis_.scard(key) > 0) continue; // 已有数据跳过
        auto members = DBManager::getInstance().getGroupMembersFromDB(groupId);
        if (!members.empty()) {
            for (int uid : members) {
                redis_.sadd(key, std::to_string(uid));
            }
            LOG_INFO("[GroupManager] 从数据库恢复群组 " + std::to_string(groupId) + " 成员 " + std::to_string(members.size()));
        }
    }
}