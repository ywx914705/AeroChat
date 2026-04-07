#include "EventLoopThread.hpp"
#include "Log.hpp"
#include <iostream>
/*
EventLoop loop;
loop对象创建在栈上,其生命周期与当前线程函数绑定.这是实现one loop per thread的核心,每个线程拥有自己独立的EventLoop对象,
且该对象的内存由线程栈所管理,无需手动分配/释放
栈对象自动销毁,避免了内存泄漏等问题
如何实现one loop per thread
1、每个线程运行一个事件循环:threadFunc()中创建了一个栈上的EventLoop对象,并立刻进入Loop,因此该线程的生命周期中只会处理
这一个事件循环
2、每个EventLoop对象管理一个线程,该线程只运行一个EventLoop,没有其他线程干扰该EventLoop的执行
3、通过runInLoop实现跨线程交互:外部通过startLoop()将任务投递到该线程执行,这些函数会检查调用线程是否为目标线程,如果不是
就放入待处理队列,由目标线程在事件循环中实行
4、EventLoop是栈上的,因此线程退出时,会自动销毁
*/
EventLoopThread::EventLoopThread(const std::string& name)
    : loop_(nullptr), exiting_(false), thread_(std::bind(&EventLoopThread::threadFunc, this), name) {}
   //创建Thread对象,线程函数为EventLoopThread::threadFunc,并传入this指针以便在线程函数中访问当前
   //对象,name是线程名称
   //注意:此时线程还没有启动,thread_对象只是构造完整,thread_.start()并没有进行调用
EventLoopThread::~EventLoopThread() {
    exiting_ = true;
    if (loop_) {
        loop_->quit();
        thread_.join();
    }
}

EventLoop* EventLoopThread::startLoop() {
    thread_.start();//启动底层线程,线程入口函是threadFunc
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return loop_ != nullptr; });
    return loop_;//一旦子线程完成初始化并通知条件变量,startLoop()返回指向该EventLoop的指针
    //此后外部就可以通过这个指针进行跨线程调用(如runInLoop)
}

void EventLoopThread::threadFunc() {
    EventLoop loop;//注意这里并不是new而是在栈上创建对象
    static int idx = 1;
    loop.setIndex(idx++);//设置索引
    
    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;//把栈对象的地址赋值给成员变量
        cond_.notify_one();//通知startLoop已经就绪
    }
    LOG_INFO("[SUCCESS] Sub Reactor \"" + thread_.name() + "\" (线程ID: " + 
             std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + ") 已启动");
    loop.loop();//进入事件循环
    //调用 EventLoop::loop(),该函数内部是一个无限循环(知道quit()被调用),处理 I/O 事件和跨线程回调。
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;//循环结束,进行清理
}