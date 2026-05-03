#ifndef SESSION_MANAGER_HPP
#define SESSION_MANAGER_HPP

#include <string>
#include <vector>
#include <tuple>
#include <unordered_map>
#include <shared_mutex>
#include "RedisClient.hpp"

struct OnlineUserInfo {
    std::string account;
    std::string username;
    std::string avatarUrl;
};

class SessionManager {
public:
    static SessionManager& instance();

    bool init(const std::string& db_host, const std::string& db_user,
              const std::string& db_passwd, const std::string& db_name,
              unsigned int db_port);
    void initEmpty();

    bool login(int fd, const std::string& account, int userId,
               const std::string& username, const std::string& avatarUrl);
    void logout(int fd);

    bool isOnline(int fd) const;
    bool isOnline(const std::string& account) const;
    int getOnlineCount() const;

    std::string getAccountByFd(int fd) const;
    int getFdByAccount(const std::string& account) const;
    std::string getUsernameByAccount(const std::string& account) const;
    std::string getUsernameByFd(int fd) const;
    std::string getAccountByUsername(const std::string& username) const;
    int getUserIdByAccount(const std::string& account) const;
    std::string getAccountByUserId(int userId) const;

    // 新增：根据账号或用户名获取真实账号（优先当作账号，失败则当作用户名）
    std::string getAccountByAccountOrUsername(const std::string& input) const;

    void setAvatar(const std::string& account, const std::string& avatarUrl);
    std::string getAvatar(const std::string& account, const std::string& defaultAvatar = "") const;
    std::string getAvatarByFd(int fd, const std::string& defaultAvatar = "") const;

    bool updateUsername(int fd, const std::string& new_username);

    std::vector<std::tuple<std::string, std::string, std::string>> getAllOnlineUserInfos() const;
    // 分页获取在线用户（使用 Sorted Set，O(logN) 复杂度）
    std::vector<OnlineUserInfo> getOnlineUsersPaginated(int offset, int limit) const;
    std::vector<OnlineUserInfo> searchOnlineUsers(const std::string& keyword, int limit = 20) const;
    std::vector<OnlineUserInfo> searchAllUsers(const std::string& keyword, int limit = 20) const;

    struct UserProfile {
        std::string account;
        std::string username;
        std::string avatarUrl;
        std::string bio;
    };
    UserProfile getUserProfile(const std::string& account) const;

    std::vector<int> getFdsByAccounts(const std::vector<std::string>& accounts) const;
    void publishOnlineUpdate(const std::string& action, const std::string& account,
                             const std::string& username, const std::string& avatarUrl);

private:
    SessionManager();
    ~SessionManager() = default;
    RedisClient& redis_;

    struct OnlineInfo {
        int fd;
        int userId;
        std::string username;
        std::string avatar;
    };
    std::unordered_map<std::string, OnlineInfo> memoryOnline_;
    mutable std::shared_mutex memoryMutex_;
};

#endif // SESSION_MANAGER_HPP