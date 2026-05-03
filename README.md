<div align="center">

# AeroChat

**高性能 C++ 聊天服务器**

![C++](https://img.shields.io/badge/C%2B%2B-11-blue?logo=cplusplus)
![Linux](https://img.shields.io/badge/Linux-Ready-green?logo=linux)
![CMake](https://img.shields.io/badge/CMake-3.10+-064F8C?logo=cmake)
![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql)
![Redis](https://img.shields.io/badge/Redis-6.x-DC382D?logo=redis)
![Vue.js](https://img.shields.io/badge/Vue.js-3.x-4FC08D?logo=vuedotjs)

基于 Reactor 模式的高并发实时通信服务器

</div>

---

## 项目简介

AeroChat 是一个参考 Muduo 与 Netty 设计思想的 C++ 高性能聊天服务器，采用 **主从 Reactor + One Loop Per Thread** 架构，实现了高并发、低延迟的实时通信。

项目集成了 MySQL 连接池、Redis 缓存与发布订阅、无锁队列、异步日志等组件，并提供基于 Vue3 + WebSocket 的现代化前端界面。

> 后端使用纯 TCP 协议，前端通过 WebSocket 进行中转通信。

## 核心特点

| 特性 | 说明 |
|------|------|
| **高性能网络模型** | 主 Reactor 监听新连接，子 Reactor 处理 I/O 与业务，充分利用多核 CPU |
| **无锁队列** | 基于 `moodycamel::ConcurrentQueue` 实现跨线程任务投递，消除锁竞争 |
| **异步任务队列** | 耗时操作通过 `AeroQueue` 投递到后台线程池，不阻塞事件循环 |
| **连接管理优化** | 统一 ConnectionManager 管理所有连接，shared_mutex 读写锁，广播按 EventLoop 分组投递 |
| **背压控制** | 单连接发送缓冲区上限 4MB，防止恶意大包耗尽内存 |
| **边缘触发** | 配合非阻塞 I/O 使用 EPOLLET，提高吞吐量 |
| **双连接池** | MySQL 128 连接 + Redis 64 连接，预创建复用，支持自动重连 |
| **异步日志** | 双缓冲区设计，后端线程定时刷盘，日志不阻塞业务 |
| **模块化设计** | 登录、单聊、群聊、离线消息、历史消息、用户资料、在线状态推送等功能解耦 |
| **Vue3 前端** | WebSocket 通信，实时展示消息、在线用户、会话列表 |

## 核心功能

> 群 = 频道（同一概念），预设 8 个频道，用户最多加入 3 个

- **用户注册/登录** — 密码 SHA256 加密存储，登录返回用户信息、在线人数及已加入频道列表
- **单聊** — 实时私信收发，对方离线时自动保存消息，上线后拉取，支持发送回执
- **频道消息** — 按频道分发，仅广播给该频道内的在线成员，离线成员自动缓存消息
- **历史消息** — 按频道或对方账号分页拉取，支持 limit/offset，按时间倒序返回
- **离线消息** — 登录时自动拉取所有离线消息，包含发送者信息、消息类型、时间戳
- **频道管理** — 查看频道列表（含成员数、最后一条消息）、查看频道成员（分页）、加入/退出
- **会话列表** — 展示最近联系人，包含最后一条消息、时间、未读数，支持私聊与频道两种类型
- **在线状态** — 实时广播上线/下线（全服通知），支持分页查询在线用户列表
- **用户资料** — 修改昵称、头像（URL），按账号或昵称模糊搜索，查看他人主页
- **心跳保活** — 客户端定时 ping，服务端回复 pong，空闲连接超过 180 秒自动断开

> 目前 AeroChat 只支持文字聊天，后续可拓展图片上传等功能。

## 技术栈

| 类别 | 技术 |
|------|------|
| 语言 | C++11/14/17 |
| 网络模型 | 主从 Reactor、epoll ET 模式、非阻塞 I/O |
| 并发 | std::thread、std::atomic、moodycamel::ConcurrentQueue |
| 数据库 | MySQL 8.0（Connector/C API）、Redis 6.0（hiredis） |
| 日志 | 自研异步日志（双缓冲） |
| 序列化 | RapidJSON |
| 前端 | Vue3 + WebSocket |
| 构建 | CMake 3.10+ |
| 系统 | Linux（CentOS / Ubuntu） |

## 架构图

<div align="center">
<img width="960" alt="architecture" src="https://github.com/user-attachments/assets/e95fea33-eb1a-4605-8af4-4f64fdc3cf03" />
</div>

---

## 快速开始

### 1. 安装依赖

```bash
# 编译工具
sudo apt-get install build-essential cmake

# MySQL 开发库
sudo apt-get install libmysqlclient-dev

# hiredis（Redis C 客户端）
sudo apt-get install libhiredis-dev

# OpenSSL（用于 SHA256 加密）
sudo apt-get install libssl-dev

# RapidJSON（头文件库）
sudo apt-get install rapidjson-dev
```

### 2. 数据库配置

确保 MySQL 中已创建数据库 `chat_db`，然后执行以下 SQL：

```sql
-- 1. 用户表
CREATE TABLE `chat_users` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `account` VARCHAR(50) NOT NULL,
  `password` VARCHAR(255) NOT NULL,
  `username` VARCHAR(50) NOT NULL,
  `avatar_url` VARCHAR(255) DEFAULT '',
  `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `bio` VARCHAR(255) DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `account` (`account`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. 单聊消息表
CREATE TABLE `chat_single_messages` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `from_account` VARCHAR(50) NOT NULL,
  `to_account` VARCHAR(50) NOT NULL,
  `from_username` VARCHAR(50) DEFAULT '',
  `avatar_url` VARCHAR(255) DEFAULT '',
  `content` TEXT NOT NULL,
  `create_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_from_account` (`from_account`),
  KEY `idx_to_account` (`to_account`),
  KEY `idx_create_time` (`create_time`),
  KEY `idx_from_to_time` (`from_account`,`to_account`,`create_time`),
  KEY `idx_to_from_time` (`to_account`,`from_account`,`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. 群聊消息表
CREATE TABLE `chat_group_messages` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `from_account` VARCHAR(50) NOT NULL,
  `group_id` BIGINT NOT NULL,
  `from_username` VARCHAR(50) DEFAULT '',
  `avatar_url` VARCHAR(255) DEFAULT '',
  `content` TEXT NOT NULL,
  `create_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_from_account` (`from_account`),
  KEY `idx_create_time` (`create_time`),
  KEY `idx_group_id` (`group_id`),
  KEY `idx_group_time` (`group_id`,`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. 群组成员关系表
CREATE TABLE `group_members` (
  `group_id` BIGINT NOT NULL,
  `user_id` BIGINT NOT NULL,
  `joined_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`group_id`,`user_id`),
  UNIQUE KEY `unique_user_group` (`user_id`,`group_id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

### 3. 克隆并编译

```bash
git clone https://github.com/ywx914705/AeroChat.git
cd AeroChat

# 清理旧构建产物（仓库包含历史 build 目录）
rm -rf build

# 编译
mkdir build && cd build
cmake .. && make -j$(nproc)
```

编译成功后会在 `build` 目录下生成可执行文件 `AeroChat`。

### 4. 配置

| 配置项 | 文件 | 默认值 |
|--------|------|--------|
| MySQL 连接 | `src/ChatServer.cc` | localhost / root / 123456 / chat_db |
| Redis 连接 | `src/main.cc` | 127.0.0.1:6379 |
| WebSocket 地址 | `index.html` | ws://your-server-ip:8000 |

### 5. 运行

```bash
# 终端 1：启动服务器（在 build 目录下）
./AeroChat 8000

# 终端 2：启动 WebSocket 代理（需配置 IP 和端口）
node tcp-ws-proxy.js

# 终端 3：启动前端（Windows 端）
npx http-server -p 3000
```

---

## 性能优化

### 基础架构优化

| 优化项 | 说明 |
|--------|------|
| **多线程模型** | 主 Reactor + 子 Reactor 线程池，线程数 = CPU 核心数 x 4 |
| **无锁队列** | 跨线程任务投递使用 ConcurrentQueue，避免锁竞争 |
| **边缘触发 + 非阻塞 I/O** | epoll ET 模式，一次读取所有数据，减少系统调用 |
| **连接管理** | 统一 ConnectionManager，shared_mutex 读写锁，广播按 EventLoop 分组投递 |
| **写缓冲区背压** | 单连接发送缓冲区上限 4MB，防止内存爆炸 |
| **异步日志** | 双缓冲区设计，localtime_r 保证线程安全，日志对业务零影响 |
| **空闲连接回收** | timerfd 定时扫描，关闭超过 180 秒未活动的连接 |

### 连接池优化

| 优化项 | 优化前 | 优化后 |
|--------|--------|--------|
| MySQL 连接池 | 500 连接（超过 max_connections） | 128 连接 |
| Redis 连接池 | 500 连接 | 64 连接 |
| 连接池耗尽 | 直接返回 nullptr，操作静默失败 | 阻塞等待 3 秒超时 |

### 缓存与批量操作优化

| 优化项 | 优化前 | 优化后 | 提升 |
|--------|--------|--------|------|
| 频道最后消息 | 每次查 8 次 MySQL | Redis HGET 缓存 | ~50ms → ~5ms |
| 在线用户分页 | HGETALL 全量加载再分页 | Sorted Set 服务端分页 | ~30ms → ~2ms |
| 离线消息拉取 | 每条消息 1-2 次 Redis 查询 | Pipeline 批量查询 | ~100ms → ~10ms |
| 离线消息推送 | 每个用户一次 RPUSH | Pipeline 一次发送 | ~200ms → ~5ms |
| 会话列表重建 | 50 个频道 = 50 次 MySQL | Redis 缓存优先读取 | ~500ms → ~10ms |

---

## 项目难点与解决方案

### 全局消息广播的性能瓶颈

**问题**：早期版本对所有在线用户统一广播，导致单连接发送阻塞、锁竞争严重。

**解决**：将消息按频道和私聊目标拆分，仅向目标频道的在线成员或指定用户发送；ConnectionManager 按 EventLoop 分组存储用户，广播时按线程分发任务，避免全局锁。

### 高并发下的连接管理锁竞争

**问题**：使用全局 unordered_map 管理连接，每次查询或遍历都需要加锁，成为性能瓶颈。

**解决**：采用 shared_mutex 读写锁，读操作并发执行；ConnectionManager 统一管理所有连接，广播时按 EventLoop 分组投递任务到对应线程，减少锁持有时间。

### 跨线程任务投递与唤醒

**问题**：其他线程需要向 I/O 线程（EventLoop）提交任务，若直接加锁可能导致死锁或延迟。

**解决**：基于 moodycamel::ConcurrentQueue 无锁队列实现 pendingFunctors_，配合 eventfd 唤醒阻塞的 epoll_wait，实现高效、无锁的跨线程调用。

### 边缘触发（ET）模式下的数据读取

**问题**：ET 模式下必须循环读取直到 EAGAIN，否则会丢失数据；但循环过多可能饿死其他连接。

**解决**：每个连接循环读取时限制单次最大读取次数，读完后立即返回事件循环，保证公平性；写操作同样循环写入，剩余数据缓冲至 writeBuf_ 并注册 EPOLLOUT。

### 离线消息存储与拉取

**问题**：消息量大时，每次都查询 MySQL 会导致登录缓慢；若全存在 Redis 又浪费内存。

**解决**：消息内容持久化到 MySQL，仅将消息 ID 列表存入 Redis 收件箱；用户登录时批量拉取 ID，再通过 Pipeline 批量查询消息内容（消除 N+1 Redis 查询），平衡存储成本与性能。

### Redis/MySQL 连接池管理

**问题**：频繁创建/释放连接开销大，且连接数过多会耗尽资源。

**解决**：分别实现 MySQL（128 连接）和 Redis（64 连接）连接池，预创建固定数量连接，使用线程安全的队列管理；Redis 连接池耗尽时阻塞等待 3 秒，不再静默失败；支持自动重连和健康检查。

### 空闲连接浪费资源

**问题**：大量僵尸连接占用 fd 和内存。

**解决**：定时器（timerfd）每 60 秒扫描所有连接，关闭超过 180 秒未活动的连接，并清理相关会话缓存。

### 集群扩展 — 跨节点在线状态广播

**问题**：扩展为集群后，在线状态需跨节点同步。

**解决**：使用 Redis Pub/Sub 订阅 `online_updates` 频道，任一节点状态变化时发布消息，所有节点接收后广播给本地客户端。在线用户列表使用 Redis Sorted Set 存储，支持服务端分页查询。

---

## 参考书籍与项目

- 《Linux 多线程服务端编程：使用 muduo C++ 网络库》— 陈硕
- 《Linux 高性能服务器编程》— 游双
- [muduo](https://github.com/chenshuo/muduo) — C++ 网络库
- [Netty](https://netty.io/) — 异步事件驱动框架
- [moodycamel::ConcurrentQueue](https://github.com/cameron314/concurrentqueue) — 无锁并发队列

## 开源协议

本项目采用 [MIT License](LICENSE) 开源协议。

## 致谢

感谢所有开源社区的贡献者，以及陈硕老师的《Linux 多线程服务端编程》给予的设计灵感。

---

<div align="center">

**AeroChat —— 让你的聊天服务飞起来**

</div>
