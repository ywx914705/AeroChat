/*
该文件本质上就是对linux posix线程的封装(pthread_create创建出来的线程)
这个类在整个项目中作为基础组件,被EventLoopThread等上层类使用
linux中posix常用函数:
pthread_create()   pthread_join()  
核心优势:支持CPU亲和性,可以将线程绑定到指定CPU核心,优化性能
不可拷贝:该AeroChat中绝大多数的类都继承自nocopyable,不支持拷贝，防止多个对象管理同一个线程
,避免资源被重复释放
这个类的可读性很高,基本上看到函数/变量名就能知道它的功能/作用
*/
#pragma once
#include "noncopyable.hpp"
#include <pthread.h>
#include <functional>
#include <string>
#include <atomic>
#include <iostream>
#include <sched.h>//绑CPU核心所需头文件

class Thread : noncopyable {
public:
    using ThreadFunc = std::function<void()>;

    explicit Thread(ThreadFunc func, const std::string& name = "Thread");
    ~Thread();

    void start();
    void join();//底层调用pthread_join()
    void bindCurrentThreadToCore(int coreId);
    //目的是让当前调用它的线程绑定到指定核心。通常我们在子线程内部调用它，
    //将自己绑定到预先设定的核心。

    void setCoreAffinity(int coreId) { coreId_ = coreId; }
    //设置成员coureId_,记录希望绑定的线程核心号,实际绑定发生在线程启动后

    pthread_t tid() const { return tid_; }
    const std::string& name() const { return name_; }
    static int numCreated() { return numCreated_.load(); }

private:
    static void* runInThread(void* arg);

    ThreadFunc func_;//线程函数
    std::string name_;//线程名字,用于日志标识
    pthread_t tid_;//线程ID
    bool started_;//bool类型,表示线程是否启动 ture:启动了 false:没有启动
    bool joined_;//是否调用join(是否调用linux中的pthrea_join()函数)
    static std::atomic_int numCreated_;//已经创建线程的数量,静态原子变量

    int coreId_ = -1;//期望绑定的 CPU 核心编号，-1 表示不绑定
};