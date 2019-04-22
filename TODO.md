* 使用无锁队列替代通道？
* 写操作统一写入日志，保证日志与redis内存的一致性
* 使用多线程客户端进行压测

* go的map不能并发读写(创建key,与读取key)，所以db层面需要一个读写锁，创建key时需要对整个map加锁
* TODO 自己重新实现一个无锁map，避免不能并发读写，不能并发创建key的问题



#### 问题
* go协程进行conn.Read(buffer)网络读取时，如果没有内容是否会阻塞
    > go的net库，使用epoll,当接收缓冲区没数据时,socket read返回-1且errno=EAGAIN。并将当前G挂起，直到netpoller检测有数据进来或者操作超时G再次激活。

