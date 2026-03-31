/*
ConnectionManager:连接管理器,每个User对应一个fd对应一个Channel,对应一个连接,ConnectionManager管理着所有的连接
ConnectionManager是AeroChat中全局管理所有客户端连接的类。它负责存储每个连接的User对象
*/
#pragma once
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <vector>
#include "User.hpp"
#include "EventLoop.hpp"

class ConnectionManager {
public:
    static ConnectionManager& instance();

    void addUser(int fd, std::shared_ptr<User> user);
    void removeUser(int fd);
    std::shared_ptr<User> getUser(int fd);

    // 批量获取用户对象（按 fd 列表）
    std::vector<std::shared_ptr<User>> getUsers(const std::vector<int>& fds);

    bool sendToUser(int fd, const std::string& message);
    void broadcastMessage(const std::string& message);

private:
    ConnectionManager() = default;

    // 快速通过 fd 查找用户
    std::unordered_map<int, std::weak_ptr<User>> users_;//存储fd->User的映射关系
    // 按 EventLoop 分组存储用户，用于广播优化
    std::unordered_map<EventLoop*, std::vector<std::weak_ptr<User>>> loopUsers_;
	//EventLoop分组,每个EventLoop管理一批用户

    mutable std::shared_mutex mutex_;   // 保护 users_ 和 loopUsers_

    static const size_t MAX_PENDING_BYTES = 4 * 1024 * 1024; // 4MB
};