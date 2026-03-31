cmake_minimum_required(VERSION 3.10)
project(AeroChat CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 使用 pkg-config 查找系统库
find_package(PkgConfig REQUIRED)

# 查找 MySQL (mysqlclient)
pkg_check_modules(MYSQL REQUIRED mysqlclient)

# 查找 hiredis
pkg_check_modules(HIREDIS REQUIRED hiredis)

# 查找 libcurl
pkg_check_modules(CURL REQUIRED libcurl)

# 查找 OpenSSL
pkg_check_modules(OPENSSL REQUIRED openssl)

# 查找线程库
find_package(Threads REQUIRED)

# 设置头文件搜索路径
include_directories(include)
include_directories(${MYSQL_INCLUDE_DIRS})
include_directories(${HIREDIS_INCLUDE_DIRS})
include_directories(${CURL_INCLUDE_DIRS})
include_directories(${OPENSSL_INCLUDE_DIRS})

# 源文件列表（根据 AeroChat 实际结构）
set(SOURCES
    src/main.cc
    src/AeroQueue.cc
    src/EventLoop.cc
    src/Channel.cc
    src/Poller.cc
    src/Log.cc
    src/ConnectionPool.cc
    src/SessionManager.cc
    src/RedisClient.cc
    src/DBManager.cc
    src/ChatServer.cc
    src/ConnectionManager.cc
    src/GroupManager.cc
    src/MessageRouter.cc
    src/MessageStore.cc
    src/User.cc
    src/EventLoopThread.cc
    src/EventLoopThreadPool.cc
    src/Thread.cc
)

# 添加可执行文件
add_executable(AeroChat ${SOURCES})

# 链接库
target_link_libraries(AeroChat
    Threads::Threads
    ${MYSQL_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    ${CURL_LIBRARIES}
    ${OPENSSL_LIBRARIES}
)

# 编译选项
target_compile_options(AeroChat PRIVATE -Wall -Wextra -O2)