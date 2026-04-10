# AeroChat —— 高性能聊天服务器

![GitHub repo size](https://img.shields.io/github/repo-size/your-username/aerochat)
![GitHub license](https://img.shields.io/github/license/your-username/aerochat)
![C++](https://img.shields.io/badge/C%2B%2B-17-blue)
![Linux](https://img.shields.io/badge/Linux-Ready-green)

AeroChat 是一个基于 **Reactor模式** 的 C++ 高性能聊天服务器，参考了 Muduo 与 Netty 的设计思想，基于One Loop Per Thread的设计理念，采用 **主从 Reactor + 多线程** 架构，实现了高并发、低延迟的实时通信。项目集成了 MySQL 连接池、Redis 缓存与发布订阅、无锁队列、异步日志等组件，并提供基于 Vue3 + WebSocket 的现代化前端界面。

> 后端使用纯 TCP 协议，前端通过 WebSocket 进行中转通信。

## ✨特点

- **高性能网络模型**：主 Reactor 仅负责监听新连接，子 Reactor 负责处理 I/O 与业务，充分利用多核 CPU。
- **无锁队列**：基于 `moodycamel::ConcurrentQueue` 实现跨线程任务投递，消除锁竞争。
- **异步任务队列**：耗时操作（数据库查询、Redis 操作）通过 `AeroQueue` 投递到后台线程池，不阻塞事件循环。
- **连接管理优化**：按 EventLoop 分组存储用户，广播时按线程分发，避免全局锁。
- **背压控制**：发送缓冲区大小限制（默认 4MB），防止恶意大包耗尽内存。
- **读写缓冲与 ET 模式**：配合非阻塞 I/O 使用边缘触发（EPOLLET），提高吞吐量。
- **MySQL 连接池**：预创建连接，支持自动重连，支持事务与预编译语句。
- **Redis 连接池**：封装 hiredis，支持字符串、集合、哈希、发布订阅等操作。
- **双缓冲区异步日志**：后端线程定时刷盘，前端写入无锁，日志不阻塞业务。
- **模块化设计**：登录、单聊、群聊、离线消息、历史消息、用户资料、在线状态推送等功能解耦。
- **前端 Vue3**：使用 WebSocket 通信，实时展示消息、在线用户、会话列表，支持头像昵称修改。

## 🛠️技术栈

| 类别       | 技术                                                         |
| ---------- | ------------------------------------------------------------ |
| 语言       | C++11以及部分c++14/c++11                                                  |
| 网络模型   | 主从Reactor,epoll（ET 模式）、非阻塞 I/O                                 |
| 并发       | std::thread、std::atomic、moodycamel::ConcurrentQueue |
| 数据库     | MySQL 8.0 + MySQL Connector/C API、Redis 6.0 + hiredis       |
| 日志       | 自研异步日志（双缓冲）                                       |
| 序列化     | RapidJSON（腾讯开源）                                        |
| 前端       | Vue3 + WebSocket（原生）                                     |
| 构建       | CMake 3.10+                                                 |
| 系统       | Linux（CentOS / Ubuntu）                                    |

## 🏗️架构图

<img width="2415" height="1171" alt="image" src="https://github.com/user-attachments/assets/e95fea33-eb1a-4605-8af4-4f64fdc3cf03" />
由于自己画架构图一个页面很难放下，所以这里借助了DeepSeek

## 🚀 快速开始

### 1. 安装依赖

### 安装编译工具
sudo apt-get install build-essential cmake

### 安装 MySQL 开发库
sudo apt-get install libmysqlclient-dev

### 安装 hiredis（Redis C 客户端）
sudo apt-get install libhiredis-dev

### 安装 OpenSSL（用于 SHA256 加密）
sudo apt-get install libssl-dev

### 安装 RapidJSON（头文件库）
sudo apt-get install rapidjson-dev
### 2. 克隆源码
git clone https://github.com/your-username/aerochat.git
<img width="1163" height="215" alt="image" src="https://github.com/user-attachments/assets/7203b123-4aa5-4f25-ab87-607f891e3b03" />

cd AeroChat
### 3. 构建build目录并编译
输入命令:  mkdir build && cd build 
<img width="1314" height="85" alt="image" src="https://github.com/user-attachments/assets/e0f9ca88-4e0e-405f-a466-6ceb28ccd2e3" />
进入build目录后执行cmake .. 

### 执行完再执行make进行编译 生成可执行文件AeroChat
<img width="840" height="278" alt="image" src="https://github.com/user-attachments/assets/ac56270f-b480-4417-8105-bfa15e1149a4" />

这个绿色的就是可执行文件,看到这个就说明成功了

# 查看日志文件
build目录下的server.log就是该服务器的日志信息
### 最后执行
./AeroChat 端口号 即可

### 4. 配置
数据库：编辑 ChatServer.cc 或 ConnectionPool.cc 中的 MySQL 连接信息（默认 localhost，root，123456，chat_db）。

Redis：默认连接 127.0.0.1:6379，如需修改请编辑 main.cc 中的 RedisClient::init 调用。

数据库表结构：确保 MySQL 中已创建数据库 chat_db，并导入 sql/schema.sql（请根据源码中的 SQL 语句自行创建表，如 chat_users、chat_group_messages、chat_single_messages、group_members 等）。

前端配置：打开index.html，修改 WebSocket 连接地址为你的服务器地址（例如 ws://your-server-ip:8000）。

### 5. 运行
在 build 目录下执行
./bin/Aerochat 8000
服务器将监听 8000 端口。
另起终端执行 node tcp-ws-proxy.js(需要配置成自己的IP以及端口)
在Windows端,打开关机旁的搜索栏输入cmd打开命令行,这里可以把index.html直接放在桌面上,在命令行中进入index.html所属路径执行npx http-server -p 3000

## ⚙️性能优化
多线程模型：主 Reactor + 子 Reactor 线程池，默认线程数 = CPU 核心数 × 2。

无锁队列：跨线程任务投递使用 ConcurrentQueue，避免锁竞争。

边缘触发 + 非阻塞 I/O：配合 epoll ET 模式，一次读取所有数据，减少系统调用。

连接管理分组：广播时按 EventLoop 分发，避免多线程同时操作同一用户的发送缓冲区。

写缓冲区背压：单连接发送缓冲区上限 4MB，防止内存爆炸。

MySQL 连接池：预创建连接，复用 TCP 连接，减少握手开销。

Redis 连接池：复用连接，支持 Pipeline 批量操作。

异步日志：后端线程定时刷盘，前端写入仅内存操作，日志对业务零影响。

CPU 亲和性绑定：线程可绑定到指定 CPU 核心，提高缓存命中率。

📚 参考书籍与项目
《Linux多线程服务端编程：使用 muduo C++网络库》—— 陈硕

《Linux高性能服务器编程》—— 游双

muduo 网络库

Netty 异步事件驱动框架

moodycamel::ConcurrentQueue

📄 开源协议
本项目采用 MIT 许可证。

👥 致谢
感谢所有开源社区的贡献者，以及陈硕老师的《Linux 多线程服务端编程》给予的设计灵感。

AeroChat —— 让你的聊天服务飞起来 ✈️
