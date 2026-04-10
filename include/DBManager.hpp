/*
DBManager是AeroChat中负责与MySQL数据库交互的统一接口类,封装了所有的数据库操作
*/
#ifndef DBMANAGER_HPP
#define DBMANAGER_HPP

#include <string>
#include <mysql/mysql.h>
#include <vector>
class DBManager {
public:
    static DBManager& getInstance();

    // 初始化数据库连接（实际使用连接池）
    bool connect(const std::string& host, const std::string& user,
                 const std::string& passwd, const std::string& db,
                 unsigned int port);

    // 获取/释放连接（从连接池）
    MYSQL* getConnection();
    void releaseConnection(MYSQL* conn);

    // 工具：SHA256哈希
    static std::string sha256(const std::string& str);

    // 用户相关
    // 登录验证
    bool queryAccount(const std::string& account, const std::string& password,
                      int& userId, std::string& username, std::string& avatarUrl);
    // 通过账号获取用户名
   // bool queryUsernameByAccount(const std::string& account, std::string& username);
   
    // 检查账号是否已存在（直接查询数据库，不返回用户名）
     bool isAccountExist(const std::string& account);
    // 更新用户名
    bool updateUsername(const std::string& account, const std::string& new_username);
    // 获取用户头像
    std::string queryUserAvatar(const std::string& account);
    // 更新用户头像
    bool updateUserAvatar(const std::string& account, const std::string& avatarUrl);
    // 注册新用户
    bool insertUser(const std::string& account, const std::string& hashedPassword,
                    const std::string& username, int& userId);

    // 群组成员相关
    // 添加群组成员（数据库）
    bool addGroupMember(int groupId, int userId);
    // 移除群组成员
    bool removeGroupMember(int groupId, int userId);
    // 检查用户是否在群组中
    bool isGroupMember(int groupId, int userId);
    // 获取群组的所有成员ID（从数据库）
    std::vector<int> getGroupMembersFromDB(int groupId);
    // 获取用户加入的所有群组ID（从数据库）
    std::vector<int> getUserGroupsFromDB(int userId);
    // 获取所有用户ID（用于初始化群组）
    std::vector<int> getAllUserIds(int limit = 100);

    // 获取最后一次错误信息
    std::string getError() const;

private:
    DBManager() = default;
    std::string error_;
};


#endif // DBMANAGER_HPP