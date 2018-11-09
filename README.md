# gedis
a slimple demo that use go to realize redis like nosql 


 使用go实现类似redis的nosql服务
 目前已经实现kv,hash,zset,set等数据类型
* 启动服务
  ```
  go run server.go
  ```
* 客户端直接使用redis-cli
  ```
  redis-cli -p 9999
  ```
### TODO
* 时间任务，清理过期key
* 页面置换算法LRU,LFU
* 主从同步
* 支持持久化
* 支持事务ACID特性
* 性能优化
* 使用协程的情况下，处理并发冲突

### DONE
* 统一db的存储结构
* 支持redis通信协议
* 使用纯go实现的跳跃表
* 支持set数据类型
* 支持list数据类型
* 默认配置启动
> 引用代码
* [https://github.com/chaozh/MIT-6.824/issues/3)
* [https://github.com/Skycrab/cham/tree/master/lib/zset)
* [https://github.com/xcltapestry/xclpkg/tree/master/algorithm)


