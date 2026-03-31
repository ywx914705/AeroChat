#pragma once

#include <hiredis/hiredis.h>
#include <string>
#include <queue>
#include <mutex>
#include <memory>
#include <vector>
#include <unordered_map>
/*
RedisClient顾名思义就是AeroChat中负责与Redis服务器交互,RedisClient封装了hiredis c库,并维护了一个连接池来
复用TCP连接
*/
#ifndef REDIS_CLIENT_HPP
#define REDIS_CLIENT_HPP

#include <hiredis/hiredis.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <queue>
#include <functional>
#include <thread>
#include <atomic>
#include <memory>

class RedisClient {
public:
    static RedisClient& instance();

    bool init(const std::string& host, int port, int poolSize = 128);
    redisContext* getContext();
    void releaseContext(redisContext* ctx);

    // 基础命令
    bool set(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    bool del(const std::string& key);
    bool expire(const std::string& key, int seconds);
    bool exists(const std::string& key);

    // Set 操作
    long long sadd(const std::string& key, const std::string& member);
    long long srem(const std::string& key, const std::string& member);
    bool sismember(const std::string& key, const std::string& member);
    std::vector<std::string> smembers(const std::string& key);
    long long scard(const std::string& key);

    // List 操作
    long long rpush(const std::string& key, const std::string& value);
    std::vector<std::string> lrange(const std::string& key, int start, int stop);
    long long llen(const std::string& key);
    bool ltrim(const std::string& key, int start, int stop);

    // Hash 操作
    bool hset(const std::string& key, const std::string& field, const std::string& value);
    std::string hget(const std::string& key, const std::string& field);
    bool hdel(const std::string& key, const std::string& field);
    bool hexists(const std::string& key, const std::string& field);
    long long hlen(const std::string& key);
    std::unordered_map<std::string, std::string> hgetall(const std::string& key);
    std::vector<std::string> hkeys(const std::string& key);
    std::vector<std::string> hvals(const std::string& key);
    long long hincrby(const std::string& key, const std::string& field, long long increment);

    // 批量操作
    std::vector<std::string> multiHget(const std::vector<std::string>& keys, const std::string& field);

    // 发布/订阅
    bool publish(const std::string& channel, const std::string& message);
    void subscribe(const std::string& channel, std::function<void(const std::string&)> callback);
    void unsubscribe();

private:
    RedisClient() = default;
    ~RedisClient() { stop(); }
    void stop();

    std::string host_;
    int port_;
    int poolSize_;
    std::queue<redisContext*> pool_;
    std::mutex mutex_;

    // 订阅相关
    std::unique_ptr<std::thread> subThread_;
    std::atomic<bool> subRunning_{false};
    std::string subChannel_;
    std::function<void(const std::string&)> subCallback_;
};

#endif // REDIS_CLIENT_HPP