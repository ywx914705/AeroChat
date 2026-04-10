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
## 💬 核心功能
下面的群和频道是一个概念！！！
- **用户认证**：支持注册、登录，密码 SHA256 加密存储，登录后返回用户信息、在线人数及已加入的群组列表
- **单聊**：实时私信收发，对方离线时自动保存消息，上线后拉取，支持发送回执
- **群聊**：预设8个频道，用户可根据自己的喜好加入不同的频道,最多加入三个频道，群消息仅广播给该群频道内的在线成员（按频道分发），离线成员自动缓存消息
- **历史消息**：按群组或对方账号分页拉取历史消息，支持 limit/offset，按时间倒序返回
- **离线消息**：登录时自动拉取所有离线消息，消息内容包含发送者信息、消息类型、时间戳
- **群组管理**：查看群组列表（含成员数、最后一条消息）、查看群成员（分页）、加入/退出群组（用户最多加入 3 个群，群上限 3000 人）
- **会话列表**：展示最近联系人，包含最后一条消息、最后时间、未读数量，支持私聊与群聊两种类型
- **在线状态**：实时广播用户上线/下线（全服通知），支持分页查询在线用户列表（默认每页 200 人）
- **用户资料**：支持修改昵称、头像（URL），只需要输入一张图片的URL就可以使用对应的头像，支持按账号或昵称模糊搜索用户，查看他人主页
- **心跳保活**：客户端定时发送 ping，服务端回复 pong，空闲连接超过 60 秒自动断开
- **消息协议**：基于JSON，每条消息以换行符 `\n` 分隔（此处保留代码格式以便说明，若不需要可去掉）
  
## 目前AeroChat只支持文字聊天,后续可以进行拓展支持图片上传等功能。

## 🛠️技术栈

| 类别       | 技术                                                         |
| ---------- | ------------------------------------------------------------ |
| 语言       | C++11以及部分c++14/c++17                                                 |
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
git clone  https://github.com/ywx914705/AeroChat.git

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

## 🧩 项目难点与解决方案
### 全局消息广播的性能瓶颈
早期版本对所有在线用户统一广播，导致单连接发送阻塞、锁竞争严重。

解决：将消息按群组频道和私聊目标拆分，仅向目标群组/频道的在线成员或指定用户发送；同时按 EventLoop 分组存储用户，广播时按线程分发任务，避免全局锁。

### 高并发下的连接管理锁竞争
使用全局 unordered_map 管理连接，每次查询或遍历都需要加锁，成为性能瓶颈。

解决：采用 shared_mutex 读写锁，读操作并发执行；并引入 loopUsers_ 按 EventLoop 分组存储，广播时直接获取目标线程的用户列表，减少锁持有时间。

### 跨线程任务投递与唤醒
其他线程需要向 I/O 线程（EventLoop）提交任务（如发送消息、关闭连接），若直接加锁可能导致死锁或延迟。

解决：基于 moodycamel::ConcurrentQueue 无锁队列实现 pendingFunctors_，配合 eventfd 唤醒阻塞的 epoll_wait，实现高效、无锁的跨线程调用。

### 边缘触发（ET）模式下的数据读取
ET 模式下必须循环读取直到 EAGAIN，否则会丢失数据；但循环过多可能饿死其他连接。

解决：每个连接循环读取时限制单次最大读取次数（或字节数），读完后立即返回事件循环，保证公平性；写操作同样循环写入，剩余数据缓冲至 writeBuf_ 并注册 EPOLLOUT。

### 离线消息存储与拉取
消息量大时，每次都查询 MySQL 会导致登录缓慢；若全存在 Redis 又浪费内存。

解决：消息内容持久化到 MySQL，仅将消息 ID 列表存入 Redis 收件箱；用户登录时批量拉取 ID，再批量从 MySQL 查询消息内容，平衡存储成本与性能。

### 发送缓冲区背压控制
恶意客户端或网络慢会导致发送缓冲区无限增长，耗尽服务器内存。

解决：每个连接限制发送缓冲区上限（4MB），超出则主动关闭连接；同时限制接收缓冲区大小，防止恶意大包。

### 异步日志的性能开销
同步写日志会阻塞 I/O 线程，尤其在高并发下严重影响吞吐。

解决：实现双缓冲区异步日志，前端写入仅追加到缓冲区，后端线程定时刷盘；支持日志轮转，避免单文件过大。

### Redis/MySQL 连接池管理
频繁创建/释放连接开销大，且连接数过多会耗尽资源。

解决：分别实现 MySQL 和 Redis 连接池，预创建固定数量连接，使用线程安全的队列管理；支持自动重连和健康检查。

### 空闲连接浪费资源
大量僵尸连接占用 fd 和内存。

解决：定时器（timerfd）每 60 秒扫描所有连接，关闭长时间未活动的连接，并清理相关会话缓存

### 若扩展为集群，在线状态需跨节点广播。

解决：使用 Redis Pub/Sub 订阅 online_updates 频道，任一节点状态变化时发布消息，所有节点接收后广播给本地客户端。

# 📚 参考书籍与项目
《Linux多线程服务端编程：使用 muduo C++网络库》—— 陈硕

《Linux高性能服务器编程》—— 游双

muduo 网络库

Netty 异步事件驱动框架

moodycamel::ConcurrentQueue

# 📄 开源协议
本项目采用 MIT 许可证。

# 👥 致谢
感谢所有开源社区的贡献者，以及陈硕老师的《Linux 多线程服务端编程》给予的设计灵感。

# AeroChat —— 让你的聊天服务飞起来 ✈️
