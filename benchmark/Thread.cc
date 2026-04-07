/*
Thread类的实现,大部分函数一看就懂,这里只重点为bindCurrentThreadToCore、runInThread
添加详细注释
bindCurrentThreadToCore:
让线程固定在某个核心上运行可以避免线程在不同核心间频繁迁移，从而提高 CPU 缓存命中率，
减少上下文切换开销
*/#define _GNU_SOURCE 1
#include "Thread.hpp"
#include "Log.hpp"
#include <cstring>
#include <cerrno>

std::atomic_int Thread::numCreated_(0);

Thread::Thread(ThreadFunc func, const std::string& name)
    : func_(std::move(func))//使用move(避免了拷贝)
    , name_(name)
    , tid_(0)
    , started_(false)
    , joined_(false) 
    , coreId_(-1) {
    if (name.empty()) {
        char buf[32];
        snprintf(buf, sizeof(buf), "Thread-%d", numCreated_.fetch_add(1) + 1);
	//numCreate_.fetch_add(1)原子递增并且返回旧值,所以+1后就是新线程的序号
        name_ = buf;
    }
}

Thread::~Thread() {
    if (started_ && !joined_) {
        pthread_detach(tid_);
    }
}

void Thread::start() {
    if (started_) return;
    started_ = true;
    int ret = pthread_create(&tid_, nullptr, runInThread, this);
	//pthread_create()的第三个参数为线程创建后即将要执行的函数,并传入Thread对象指针this
    if (ret != 0) {
        LOG_ERROR("pthread_create failed! ret=" + std::to_string(ret));
        abort();
    }
}

void Thread::join() {
    if (!started_ || joined_) return;
    joined_ = true;
    pthread_join(tid_, nullptr);
}

void Thread::bindCurrentThreadToCore(int coreId) {
//这是一个成员函数,此函数的作用是让当前调用它的线程绑定到指定CPU核心,通常我们在子线程内部
//调用它,将自己绑定到预先设定的核心
//这样做有什么好处？可以提高CPU缓存命中率,篇幅有限,这里只是简单的叙述,以及减少上下文切换
    cpu_set_t cpuset;//声明CPU集合
    CPU_ZERO(&cpuset);//清空集合
    CPU_SET(coreId, &cpuset);//设置指定核心
    pthread_t current = pthread_self();
	//pthread_self() 返回当前线程的pthread句柄类型为pthread_t用于标识要绑定的线程。
    //注意:这里的“当前线程”是指执行该函数的线程,也就是我们想要绑定的那个线程本身。
    int ret = pthread_setaffinity_np(current, sizeof(cpu_set_t), &cpuset);
	//pthread_setaffinity_np 是一个 非标准(np = non-portable)的Linux函数,
	//用于设置线程的 CPU 亲和性
    if (ret != 0) {
        LOG_WARN("警告：绑定线程 " + name_ + " 到CPU " + std::to_string(coreId) + " 失败，错误码：" + std::to_string(ret));
    }
}

void* Thread::runInThread(void* arg) {
    Thread* thread = static_cast<Thread*>(arg);//将void*类型转换为Thread*类型
    if (thread->coreId_ >= 0) {
		//如果设置了coreId_,则调用bindCurrentThreadToCore将当前线程(即新创建的线程)
		//绑定到指定CPU
        thread->bindCurrentThreadToCore(thread->coreId_);
    }
    thread->func_();//执行用户提供的func_ 在这里就是EventLoopThread::threadFunc,然后
    //EventLoopThread::threadFunc就在子线程中运行,创建并运行EventLoop。
    return nullptr;
}