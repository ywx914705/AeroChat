#include "RedisClient.hpp"
#include "Log.hpp"
#include <cstring>
#include <iostream>

RedisClient& RedisClient::instance() {
    static RedisClient instance;
    return instance;
}

bool RedisClient::init(const std::string& host, int port, int poolSize) {
    host_ = host;
    port_ = port;
    poolSize_ = poolSize;

    for (int i = 0; i < poolSize_; ++i) {
        redisContext* ctx = redisConnect(host_.c_str(), port_);
        if (!ctx || ctx->err) {
            if (ctx) {
                LOG_ERROR("[Redis] Connection error: " + std::string(ctx->errstr));
                redisFree(ctx);
            } else {
                LOG_ERROR("[Redis] Cannot allocate redis context");
            }
            return false;
        }
        pool_.push(ctx);
    }
    LOG_INFO("[Redis] 连接池初始化完成，创建 " + std::to_string(poolSize_) + " 个连接");
    return true;
}

redisContext* RedisClient::getContext() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pool_.empty()) {
        return nullptr;
    }
    redisContext* ctx = pool_.front();
    pool_.pop();
    return ctx;
}

void RedisClient::releaseContext(redisContext* ctx) {
    if (!ctx) return;
    std::lock_guard<std::mutex> lock(mutex_);
    pool_.push(ctx);
}

bool RedisClient::set(const std::string& key, const std::string& value) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SET %s %s", key.c_str(), value.c_str());
    bool ok = (reply && reply->type != REDIS_REPLY_ERROR);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

std::string RedisClient::get(const std::string& key) {
    redisContext* ctx = getContext();
    if (!ctx) return "";
    redisReply* reply = (redisReply*)redisCommand(ctx, "GET %s", key.c_str());
    std::string val;
    if (reply && reply->type == REDIS_REPLY_STRING) {
        val = std::string(reply->str, reply->len);
    }
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return val;
}

bool RedisClient::del(const std::string& key) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "DEL %s", key.c_str());
    bool ok = (reply && reply->integer > 0);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

bool RedisClient::expire(const std::string& key, int seconds) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "EXPIRE %s %d", key.c_str(), seconds);
    bool ok = (reply && reply->integer == 1);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

bool RedisClient::exists(const std::string& key) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "EXISTS %s", key.c_str());
    bool ok = (reply && reply->integer > 0);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

long long RedisClient::sadd(const std::string& key, const std::string& member) {
    redisContext* ctx = getContext();
    if (!ctx) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SADD %s %s", key.c_str(), member.c_str());
    long long ret = (reply ? reply->integer : -1);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ret;
}

long long RedisClient::srem(const std::string& key, const std::string& member) {
    redisContext* ctx = getContext();
    if (!ctx) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SREM %s %s", key.c_str(), member.c_str());
    long long ret = (reply ? reply->integer : -1);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ret;
}

bool RedisClient::sismember(const std::string& key, const std::string& member) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SISMEMBER %s %s", key.c_str(), member.c_str());
    bool ok = (reply && reply->integer == 1);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

std::vector<std::string> RedisClient::smembers(const std::string& key) {
    redisContext* ctx = getContext();
    std::vector<std::string> result;
    if (!ctx) return result;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SMEMBERS %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < reply->elements; ++i) {
            result.emplace_back(reply->element[i]->str, reply->element[i]->len);
        }
    }
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return result;
}

long long RedisClient::scard(const std::string& key) {
    redisContext* ctx = getContext();
    if (!ctx) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SCARD %s", key.c_str());
    long long ret = (reply ? reply->integer : -1);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ret;
}

// SSCAN 实现
size_t RedisClient::sscan(const std::string& key, size_t cursor, std::vector<std::string>& result, int count) {
    redisContext* ctx = getContext();
    if (!ctx) return 0;
    redisReply* reply = (redisReply*)redisCommand(ctx, "SSCAN %s %zu COUNT %d", key.c_str(), cursor, count);
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
        if (reply) freeReplyObject(reply);
        releaseContext(ctx);
        return 0;
    }
    // 解析 cursor
    size_t next_cursor = 0;
    if (reply->element[0]->type == REDIS_REPLY_STRING) {
        next_cursor = std::stoull(reply->element[0]->str);
    }
    // 解析结果数组
    redisReply* elements = reply->element[1];
    if (elements->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < elements->elements; ++i) {
            if (elements->element[i]->type == REDIS_REPLY_STRING) {
                result.emplace_back(elements->element[i]->str, elements->element[i]->len);
            }
        }
    }
    freeReplyObject(reply);
    releaseContext(ctx);
    return next_cursor;
}

long long RedisClient::rpush(const std::string& key, const std::string& value) {
    redisContext* ctx = getContext();
    if (!ctx) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "RPUSH %s %s", key.c_str(), value.c_str());
    long long ret = (reply ? reply->integer : -1);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ret;
}

std::vector<std::string> RedisClient::lrange(const std::string& key, int start, int stop) {
    redisContext* ctx = getContext();
    std::vector<std::string> result;
    if (!ctx) return result;
    redisReply* reply = (redisReply*)redisCommand(ctx, "LRANGE %s %d %d", key.c_str(), start, stop);
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < reply->elements; ++i) {
            result.emplace_back(reply->element[i]->str, reply->element[i]->len);
        }
    }
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return result;
}

long long RedisClient::llen(const std::string& key) {
    redisContext* ctx = getContext();
    if (!ctx) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "LLEN %s", key.c_str());
    long long ret = (reply ? reply->integer : -1);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ret;
}

bool RedisClient::ltrim(const std::string& key, int start, int stop) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "LTRIM %s %d %d", key.c_str(), start, stop);
    bool ok = (reply && reply->type != REDIS_REPLY_ERROR);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

bool RedisClient::hset(const std::string& key, const std::string& field, const std::string& value) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
    bool ok = (reply && reply->type != REDIS_REPLY_ERROR);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

std::string RedisClient::hget(const std::string& key, const std::string& field) {
    redisContext* ctx = getContext();
    if (!ctx) return "";
    redisReply* reply = (redisReply*)redisCommand(ctx, "HGET %s %s", key.c_str(), field.c_str());
    std::string val;
    if (reply && reply->type == REDIS_REPLY_STRING) {
        val = std::string(reply->str, reply->len);
    }
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return val;
}

bool RedisClient::hdel(const std::string& key, const std::string& field) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HDEL %s %s", key.c_str(), field.c_str());
    bool ok = (reply && reply->integer > 0);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

bool RedisClient::hexists(const std::string& key, const std::string& field) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HEXISTS %s %s", key.c_str(), field.c_str());
    bool ok = (reply && reply->integer == 1);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

long long RedisClient::hlen(const std::string& key) {
    redisContext* ctx = getContext();
    if (!ctx) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HLEN %s", key.c_str());
    long long ret = (reply && reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ret;
}

std::unordered_map<std::string, std::string> RedisClient::hgetall(const std::string& key) {
    redisContext* ctx = getContext();
    std::unordered_map<std::string, std::string> result;
    if (!ctx) return result;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HGETALL %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < reply->elements; i += 2) {
            std::string field(reply->element[i]->str, reply->element[i]->len);
            std::string value(reply->element[i+1]->str, reply->element[i+1]->len);
            result[field] = value;
        }
    }
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return result;
}

std::vector<std::string> RedisClient::hkeys(const std::string& key) {
    redisContext* ctx = getContext();
    std::vector<std::string> result;
    if (!ctx) return result;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HKEYS %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < reply->elements; ++i) {
            result.emplace_back(reply->element[i]->str, reply->element[i]->len);
        }
    }
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return result;
}

std::vector<std::string> RedisClient::hvals(const std::string& key) {
    redisContext* ctx = getContext();
    std::vector<std::string> result;
    if (!ctx) return result;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HVALS %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < reply->elements; ++i) {
            result.emplace_back(reply->element[i]->str, reply->element[i]->len);
        }
    }
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return result;
}

std::vector<std::string> RedisClient::multiHget(const std::vector<std::string>& keys, const std::string& field) {
    std::vector<std::string> result;
    if (keys.empty()) return result;
    result.resize(keys.size());

    redisContext* ctx = getContext();
    if (!ctx) return result;

    for (const auto& key : keys) {
        redisAppendCommand(ctx, "HGET %s %s", key.c_str(), field.c_str());
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        redisReply* reply = nullptr;
        if (redisGetReply(ctx, (void**)&reply) == REDIS_OK && reply) {
            if (reply->type == REDIS_REPLY_STRING) {
                result[i] = std::string(reply->str, reply->len);
            }
            freeReplyObject(reply);
        }
    }
    releaseContext(ctx);
    return result;
}

long long RedisClient::hincrby(const std::string& key, const std::string& field, long long increment) {
    redisContext* ctx = getContext();
    if (!ctx) return -1;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HINCRBY %s %s %lld", key.c_str(), field.c_str(), increment);
    long long ret = (reply && reply->type == REDIS_REPLY_INTEGER) ? reply->integer : -1;
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ret;
}

bool RedisClient::publish(const std::string& channel, const std::string& message) {
    redisContext* ctx = getContext();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(ctx, "PUBLISH %s %s", channel.c_str(), message.c_str());
    bool ok = (reply && reply->type != REDIS_REPLY_ERROR);
    if (reply) freeReplyObject(reply);
    releaseContext(ctx);
    return ok;
}

void RedisClient::subscribe(const std::string& channel, std::function<void(const std::string&)> callback) {
    if (subRunning_) {
        LOG_WARN("[Redis] subscribe already running, unsubscribe first");
        return;
    }
    subChannel_ = channel;
    subCallback_ = callback;
    subRunning_ = true;
    subThread_ = std::make_unique<std::thread>([this]() {
        redisContext* ctx = redisConnect(host_.c_str(), port_);
        if (!ctx || ctx->err) {
            LOG_ERROR(std::string("[Redis] subscribe connect failed: ") + (ctx ? ctx->errstr : "null"));
            if (ctx) redisFree(ctx);
            subRunning_ = false;
            return;
        }
        redisReply* reply = (redisReply*)redisCommand(ctx, "SUBSCRIBE %s", subChannel_.c_str());
        freeReplyObject(reply);
        while (subRunning_) {
            redisReply* msg = nullptr;
            if (redisGetReply(ctx, (void**)&msg) == REDIS_OK) {
                if (msg && msg->type == REDIS_REPLY_ARRAY && msg->elements == 3) {
                    std::string message(msg->element[2]->str, msg->element[2]->len);
                    if (subCallback_) {
                        subCallback_(message);
                    }
                }
                freeReplyObject(msg);
            } else {
                break;
            }
        }
        redisFree(ctx);
        LOG_INFO("[Redis] subscribe thread stopped");
    });
}

void RedisClient::unsubscribe() {
    subRunning_ = false;
    if (subThread_ && subThread_->joinable()) {
        subThread_->join();
    }
    subThread_.reset();
}

void RedisClient::stop() {
    unsubscribe();
    std::lock_guard<std::mutex> lock(mutex_);
    while (!pool_.empty()) {
        redisFree(pool_.front());
        pool_.pop();
    }
}