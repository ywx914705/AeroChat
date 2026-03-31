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
    bool connect(const std::string& host, const std::string& user,
                 const std::string& passwd, const std::string& db,
                 unsigned int port);
    MYSQL* getConnection();
    void releaseConnection(MYSQL* conn);
    static std::string sha256(const std::string& str);

    // 用户相关
    bool queryAccount(const std::string& account, const std::string& password,
                      int& userId, std::string& username, std::string& avatarUrl);
    bool queryUsernameByAccount(const std::string& account, std::string& username);
    bool updateUsername(const std::string& account, const std::string& new_username);
    std::string queryUserAvatar(const std::string& account);
    bool updateUserAvatar(const std::string& account, const std::string& avatarUrl);
    bool insertUser(const std::string& account, const std::string& hashedPassword,
                    const std::string& username, int& userId);

    // 群组成员相关（新增）
    bool addGroupMember(int groupId, int userId);
    bool removeGroupMember(int groupId, int userId);
    bool isGroupMember(int groupId, int userId);
    std::vector<int> getGroupMembersFromDB(int groupId);

    std::string getError() const;
    std::vector<int> getAllUserIds(int limit = 100);

private:
    DBManager() = default;
    std::string error_;
};

#endif