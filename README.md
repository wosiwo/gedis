# gedis
a slimple demo that use go to realize redis like nosql 


 使用go实现类似redis的nosql服务
 目前已经实现kv,hash,zset等数据类型
* 启动服务
  ```
  go run server.go
  ```
* 客户端直接使用redis-cli
  ```
  redis-cli -p 9999
  ```
### TODO
* 实现redis中db的数据结构
* 时间任务，清理过期key
* 主从同步
* 支持持久化
* 支持事务ACID特性
* 性能优化
### DONE
* 支持redis通信协议
* 使用纯go实现跳跃表
* 支持set数据类型
> 引用代码
* [https://github.com/chaozh/MIT-6.824/issues/3)
* [https://github.com/Skycrab/cham/tree/master/lib/zset)
* [https://github.com/xcltapestry/xclpkg/tree/master/algorithm)


