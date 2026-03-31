#ifndef SESSION_MANAGER_HPP
#define SESSION_MANAGER_HPP

#include <string>
#include <vector>
#include <tuple>
#include <unordered_map>
#include "RedisClient.hpp"

class SessionManager {
public:
    static SessionManager& instance();

    bool init(const std::string& db_host, const std::string& db_user,
              const std::string& db_passwd, const std::string& db_name,
              unsigned int db_port);
    void init(); // 空初始化，保持接口兼容

    // 登录/登出
    bool login(int fd, const std::string& account, const std::string& password,
               std::string& username, std::string& avatarUrl);
    bool login(int fd, const std::string& account, int userId,
               const std::string& username, const std::string& avatarUrl);
    void logout(int fd);

    // 状态查询
    bool isOnline(int fd) const;
    bool isOnline(const std::string& account) const;
    int getOnlineCount() const;

    // 映射查询
    std::string getAccountByFd(int fd) const;
    int getFdByAccount(const std::string& account) const;
    std::string getUsernameByAccount(const std::string& account) const;
    std::string getUsernameByFd(int fd) const;
    std::string getAccountByUsername(const std::string& username) const;
    int getUserIdByAccount(const std::string& account) const;
    std::string getAccountByUserId(int userId) const;

    // 头像
    void setAvatar(const std::string& account, const std::string& avatarUrl);
    std::string getAvatar(const std::string& account, const std::string& defaultAvatar = "") const;
    std::string getAvatarByFd(int fd, const std::string& defaultAvatar = "") const;

    // 用户名更新
    bool updateUsername(int fd, const std::string& new_username);

    // 获取在线用户信息列表（全量，慎用）
    std::vector<std::tuple<std::string, std::string, std::string>> getAllOnlineUserInfos() const;

    // 搜索在线用户（前缀匹配）
    std::vector<std::tuple<std::string, std::string, std::string>> searchOnlineUsers(const std::string& keyword, int limit = 20) const;

    // 用户主页信息
    struct UserProfile {
        std::string account;
        std::string username;
        std::string avatarUrl;
        std::string bio;
    };
    UserProfile getUserProfile(const std::string& account) const;

    // 批量获取 fd 列表
    std::vector<int> getFdsByAccounts(const std::vector<std::string>& accounts) const;

    // 发布在线状态变更
    void publishOnlineUpdate(const std::string& action, const std::string& account,
                             const std::string& username, const std::string& avatarUrl);

private:
    SessionManager();
    ~SessionManager() = default;
    RedisClient& redis_;
};

#endif // SESSION_MANAGER_HPP