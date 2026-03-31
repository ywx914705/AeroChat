#pragma once
// 禁止拷贝的基类（参考了Muduo核心设计，避免浅拷贝资源错误）
/*
作为一个基类,任何继承自noncopyable的类都无法被拷贝构造与拷贝复制,这是为了防止那些拥有资源的
对象被错误拷贝,进而发生析构时重复释放的错误
AeroChat中大部分的类都基层自noncopyable(不得拷贝)
*/
class noncopyable {
protected:
    noncopyable() = default;
    ~noncopyable() = default;
    noncopyable(const noncopyable&) = delete;
    noncopyable& operator=(const noncopyable&) = delete;
};