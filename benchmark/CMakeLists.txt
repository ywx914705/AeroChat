cmake_minimum_required(VERSION 3.10)
project(AeroChatNetTest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Release 优化
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pthread")

# 列出所有源文件（仅网络核心，无业务模块）
set(SOURCES
    Channel.cc
    chatserver.cc
    EventLoop.cc
    EventLoopThread.cc
    EventLoopThreadPool.cc
    Log.cc
    main.cc
    Poller.cc
    Thread.cc
    User.cc
)

# 头文件目录（当前目录）
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# 生成可执行文件
add_executable(aerochat_net_test ${SOURCES})

# 链接 pthread
target_link_libraries(aerochat_net_test pthread)

# 可选：如果需要禁用日志输出以提升性能，可以取消下面这行的注释
# target_compile_definitions(aerochat_net_test PRIVATE DISABLE_ASYNC_LOG)