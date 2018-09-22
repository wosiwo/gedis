# gedis
a slimple demo that use go to realize redis like nosql 


 使用go实现类似redis的nosql服务
 目前已经实现kv,hash,zset等数据类型
* 启动服务
  ```
  go run kv.go
  ```
* 客户端调用
  ```
  go run kvClient.go
  ```
### TODO
* 实现redis中db的数据结构
* 支持set数据类型
* 支持持久化
* 支持事务ACID特性
* 性能优化
### DONE
* 支持redis通信协议

> 引用代码
* [https://github.com/chaozh/MIT-6.824/issues/3)
* [https://github.com/Skycrab/cham/tree/master/lib/zset)


