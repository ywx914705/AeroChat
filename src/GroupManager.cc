#include "GroupManager.hpp"
#include "DBManager.hpp"
#include "Log.hpp"

GroupManager &GroupManager::instance() {
  static GroupManager instance;
  return instance;
}

std::vector<int> GroupManager::getGroupMembers(int groupId) {
  std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
  auto membersStr = redis_.smembers(key); // 从 Redis 获取字符串列表
  std::vector<int> result;

  if (membersStr.empty()) {
    // 缓存未命中，从数据库加载 int 列表
    std::vector<int> membersInt =
        DBManager::getInstance().getGroupMembersFromDB(groupId);
    if (!membersInt.empty()) {
      // 回填 Redis（转为字符串）
      for (int uid : membersInt) {
        redis_.sadd(key, std::to_string(uid));
      }
      LOG_INFO("[GroupManager] 从 DB 加载群组 " + std::to_string(groupId) +
               " 成员 " + std::to_string(membersInt.size()));
      result = std::move(membersInt);
    } else {
      // 可能该群组没有成员（空群组），但默认群组不应该为空，此处忽略
      LOG_WARN("[GroupManager] 群组 " + std::to_string(groupId) +
               " 在 DB 中无成员");
    }
  } else {
    // 将字符串列表转为 int
    for (const auto &m : membersStr) {
      result.push_back(std::stoi(m));
    }
  }
  LOG_INFO("[GroupManager] getGroupMembers group=" + std::to_string(groupId) +
           " count=" + std::to_string(result.size()));
  return result;
}

bool GroupManager::isInGroup(int groupId, int userId) {
  std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
  bool inRedis = redis_.sismember(key, std::to_string(userId));
  if (!inRedis) {
    // 查数据库
    inRedis = DBManager::getInstance().isGroupMember(groupId, userId);
    if (inRedis) {
      redis_.sadd(key, std::to_string(userId));
    }
  }
  return inRedis;
}

void GroupManager::initDefaultGroup() {
  // 从数据库获取实际存在的用户 ID（最多取前100个）
  std::vector<int> userIds = DBManager::getInstance().getAllUserIds(100);
  if (userIds.empty()) {
    LOG_WARN("[GroupManager] 没有用户，跳过初始化默认群组");
    return;
  }

  for (int groupId = 1; groupId <= 3; ++groupId) {
    std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
    long long count = redis_.scard(key);
    // 如果 Redis 中成员少于 3（可根据需要调整），则用实际用户填充
    if (count < 3) {
      for (int uid : userIds) {
        addUserToGroup(groupId, uid);
      }
      LOG_INFO("[GroupManager] 初始化群组 " + std::to_string(groupId) +
               "，添加了 " + std::to_string(userIds.size()) + " 个用户");
    } else {
      LOG_INFO("[GroupManager] 群组 " + std::to_string(groupId) +
               " 已有成员，无需初始化");
    }
  }
}
bool GroupManager::addUserToGroup(int groupId, int userId) {
  std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
  // 先写数据库
  bool dbOk = DBManager::getInstance().addGroupMember(groupId, userId);
  if (!dbOk) {
    LOG_ERROR("[GroupManager] 数据库添加群组成员失败 group=" +
              std::to_string(groupId) + " user=" + std::to_string(userId));
    return false;
  }
  // 再写 Redis
  long long ret = redis_.sadd(key, std::to_string(userId));
  if (ret > 0) {
    LOG_INFO("[GroupManager] 用户 " + std::to_string(userId) + " 加入群组 " +
             std::to_string(groupId));
    return true;
  } else {
    // 如果 Redis 失败但数据库成功，理论上应保持一致性，此处简单记录警告
    LOG_WARN("[GroupManager] 用户 " + std::to_string(userId) + " 已在群组 " +
             std::to_string(groupId) + " 中或 Redis 添加失败");
    return true; // 数据库已成功，认为成功
  }
}

void GroupManager::loadAllGroupsFromDB() {
  // 这里可以遍历所有可能的群组，从数据库加载成员并写入 Redis
  // 实际实现需要知道有哪些群组，可以从另一个表获取，简化起见，我们只加载预设的群组
  // 1-3
  for (int groupId = 1; groupId <= 3; ++groupId) {
    std::string key = GROUP_KEY_PREFIX + std::to_string(groupId);
    // 如果 Redis 已有数据，跳过
    if (redis_.scard(key) > 0)
      continue;
    auto members = DBManager::getInstance().getGroupMembersFromDB(groupId);
    if (!members.empty()) {
      for (int uid : members) {
        redis_.sadd(key, std::to_string(uid));
      }
      LOG_INFO("[GroupManager] 从数据库恢复群组 " + std::to_string(groupId) +
               " 成员 " + std::to_string(members.size()));
    }
  }
}