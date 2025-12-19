# Redis 复现路线图 (Roadmap)

本文档规划了完整复现 Redis 的后续开发计划。

## ✅ 已完成

### 第一阶段：核心数据结构实现
- [x] **String (SDS)** - 完整实现，包括多种 header 类型、预分配、惰性释放
- [x] **List (Quicklist + Listpack)** - 基础实现，支持编码转换
- [x] **Set (Intset + Hashtable)** - 完整实现，支持集合运算
- [x] **ZSet (Skiplist + Dict)** - 完整实现，支持范围查询
- [x] **Hash (Listpack + Dict)** - 完整实现，支持字段操作
- [x] **编码类型系统** - 统一的编码类型定义

### 第二阶段：存储系统
- [x] **对象系统 (robj)** - 统一的对象表示
- [x] **数据库系统 (redisDb)** - 键值对存储、过期机制
- [x] **服务器系统 (RedisServer)** - 多数据库管理
- [x] **并发安全** - 读写锁保护
- [x] **使用示例** - 完整的使用示例

## 🚧 进行中

### 第三阶段：完善数据结构实现

#### 3.1 Listpack 完整实现
- [ ] **完整的序列化格式** - 实现 Redis listpack 的完整二进制格式
  - [ ] 变长编码（varint）
  - [ ] 字符串编码
  - [ ] 整数编码
  - [ ] 总长度字段
  - [ ] 结束标记
- [ ] **完整的反序列化** - 从二进制数据恢复 listpack
- [ ] **双向遍历** - 支持向前和向后遍历
- [ ] **插入/删除优化** - 高效的插入和删除操作

#### 3.2 Skiplist 完整实现
- [ ] **完整的查找算法** - 实现标准的 skiplist 查找
- [ ] **完整的插入算法** - 包括层数随机生成、指针更新
- [ ] **完整的删除算法** - 包括指针更新、层数调整
- [ ] **范围查询优化** - 高效的 ZRANGE、ZRANGEBYSCORE
- [ ] **排名计算** - 使用 span 字段优化排名计算

#### 3.3 Dict (Hashtable) 完整实现
- [ ] **渐进式 rehash** - 实现 Redis 的渐进式 rehash 机制
- [ ] **哈希函数** - 实现 siphash 或类似的高质量哈希函数
- [ ] **冲突处理** - 链式哈希表
- [ ] **自动扩容/缩容** - 根据负载因子自动调整大小

## 📋 待开发

### 第四阶段：命令系统

#### 4.1 命令解析器
- [ ] **RESP 协议解析** - 实现 Redis 的 RESP (REdis Serialization Protocol)
  - [ ] 简单字符串 (Simple String)
  - [ ] 错误 (Error)
  - [ ] 整数 (Integer)
  - [ ] 批量字符串 (Bulk String)
  - [ ] 数组 (Array)
- [ ] **命令解析** - 解析客户端命令
- [ ] **参数验证** - 验证命令参数

#### 4.2 命令实现
- [ ] **String 命令**
  - [ ] SET、GET、SETEX、SETNX
  - [ ] MSET、MGET
  - [ ] INCR、DECR、INCRBY、DECRBY
  - [ ] APPEND、STRLEN
  - [ ] GETRANGE、SETRANGE
- [ ] **List 命令**
  - [ ] LPUSH、RPUSH、LPOP、RPOP
  - [ ] LLEN、LINDEX、LRANGE
  - [ ] LINSERT、LREM、LSET、LTRIM
  - [ ] RPOPLPUSH、BLPOP、BRPOP
- [ ] **Set 命令**
  - [ ] SADD、SREM、SMEMBERS
  - [ ] SCARD、SISMEMBER、SRANDMEMBER
  - [ ] SINTER、SUNION、SDIFF
  - [ ] SINTERSTORE、SUNIONSTORE、SDIFFSTORE
  - [ ] SMOVE、SPOP
- [ ] **ZSet 命令**
  - [ ] ZADD、ZREM、ZSCORE
  - [ ] ZCARD、ZCOUNT、ZRANK、ZREVRANK
  - [ ] ZRANGE、ZREVRANGE、ZRANGEBYSCORE
  - [ ] ZINCRBY、ZINTERSTORE、ZUNIONSTORE
  - [ ] ZPOPMAX、ZPOPMIN、BZPOPMAX、BZPOPMIN
- [ ] **Hash 命令**
  - [ ] HSET、HGET、HDEL、HEXISTS
  - [ ] HLEN、HKEYS、HVALS、HGETALL
  - [ ] HMSET、HMGET
  - [ ] HINCRBY、HINCRBYFLOAT
  - [ ] HSCAN
- [ ] **通用命令**
  - [ ] DEL、EXISTS、TYPE、KEYS
  - [ ] EXPIRE、TTL、PERSIST、PEXPIRE
  - [ ] RENAME、RENAMENX
  - [ ] SELECT、DBSIZE、FLUSHDB、FLUSHALL
  - [ ] RANDOMKEY、SCAN

#### 4.3 命令路由
- [ ] **命令表** - 命令名称到处理函数的映射
- [ ] **命令分类** - 读命令、写命令、管理命令
- [ ] **命令标志** - 是否需要参数、是否阻塞等

### 第五阶段：网络层

#### 5.1 TCP 服务器
- [ ] **TCP 监听** - 监听指定端口
- [ ] **连接管理** - 管理客户端连接
- [ ] **事件循环** - 使用 epoll/kqueue 或 Go 的 net 包
- [ ] **多客户端支持** - 支持多个并发客户端

#### 5.2 客户端处理
- [ ] **连接建立** - 接受新连接
- [ ] **数据接收** - 从客户端接收数据
- [ ] **命令执行** - 执行命令并返回结果
- [ ] **响应发送** - 将结果发送给客户端
- [ ] **连接关闭** - 处理客户端断开

#### 5.3 协议处理
- [ ] **请求解析** - 解析 RESP 格式的请求
- [ ] **响应构建** - 构建 RESP 格式的响应
- [ ] **错误处理** - 处理协议错误

### 第六阶段：高级特性

#### 6.1 持久化
- [ ] **RDB 持久化**
  - [ ] RDB 文件格式
  - [ ] 快照生成 (SAVE、BGSAVE)
  - [ ] 数据加载
  - [ ] 压缩
- [ ] **AOF 持久化**
  - [ ] AOF 文件格式
  - [ ] 命令追加
  - [ ] AOF 重写 (BGREWRITEAOF)
  - [ ] AOF 加载

#### 6.2 事务
- [ ] **MULTI/EXEC** - 事务支持
- [ ] **WATCH** - 乐观锁
- [ ] **DISCARD** - 取消事务
- [ ] **事务隔离** - 保证事务的原子性

#### 6.3 发布订阅
- [ ] **PUBLISH** - 发布消息
- [ ] **SUBSCRIBE/UNSUBSCRIBE** - 订阅/取消订阅
- [ ] **PSUBSCRIBE/PUNSUBSCRIBE** - 模式订阅
- [ ] **PUBSUB** - 查看订阅信息

#### 6.4 阻塞命令
- [ ] **BLPOP/BRPOP** - 阻塞式列表弹出
- [ ] **BZPOPMAX/BZPOPMIN** - 阻塞式有序集合弹出
- [ ] **阻塞队列管理** - 管理等待的客户端

### 第七阶段：性能优化

#### 7.1 内存优化
- [ ] **对象共享** - 共享小整数、空字符串等
- [ ] **内存压缩** - quicklist 节点压缩
- [ ] **内存统计** - INFO memory 命令

#### 7.2 性能优化
- [ ] **批量操作优化** - 批量命令的优化
- [ ] **管道支持** - 支持管道命令
- [ ] **连接池** - 客户端连接池

#### 7.3 监控和统计
- [ ] **INFO 命令** - 服务器信息
- [ ] **慢查询日志** - 记录慢查询
- [ ] **命令统计** - 统计命令执行次数和时间

### 第八阶段：集群和复制（可选）

#### 8.1 主从复制
- [ ] **复制协议** - 实现 Redis 复制协议
- [ ] **全量同步** - RDB 文件传输
- [ ] **增量同步** - 命令传播
- [ ] **复制状态管理** - 管理主从关系

#### 8.2 集群模式（高级）
- [ ] **哈希槽** - 16384 个哈希槽
- [ ] **节点发现** - 节点间的发现和通信
- [ ] **数据迁移** - 槽迁移
- [ ] **故障转移** - 主节点故障时的处理

## 🎯 优先级建议

### 高优先级（核心功能）
1. **完善 Listpack 实现** - 这是很多数据结构的基础
2. **完善 Skiplist 实现** - ZSet 的核心
3. **命令系统** - 让系统可用
4. **网络层** - 支持客户端连接

### 中优先级（重要功能）
5. **持久化** - RDB 和 AOF
6. **事务** - 保证数据一致性
7. **阻塞命令** - 完整的 List 和 ZSet 功能

### 低优先级（增强功能）
8. **发布订阅** - 消息传递
9. **性能优化** - 提升性能
10. **集群和复制** - 分布式支持

## 📚 学习资源

### Redis 源码阅读
- [Redis 源码](https://github.com/redis/redis)
- [Redis 文档](https://redis.io/docs/)
- [Redis 设计与实现](https://github.com/huangz1990/redisbook)

### 关键技术点
1. **Listpack 格式** - `redis-sourceCode/src/listpack.c`
2. **Skiplist 实现** - `redis-sourceCode/src/t_zset.c`
3. **Dict 实现** - `redis-sourceCode/src/dict.c`
4. **RESP 协议** - `redis-sourceCode/src/networking.c`
5. **命令处理** - `redis-sourceCode/src/server.c`

## 🛠️ 开发建议

### 开发顺序
1. **先完善数据结构** - 确保底层数据结构正确
2. **再实现命令系统** - 在数据结构基础上实现命令
3. **最后添加网络层** - 让系统可以通过网络访问

### 测试策略
- **单元测试** - 每个数据结构都要有完整的测试
- **集成测试** - 测试命令系统
- **性能测试** - 对比 Redis 的性能

### 代码质量
- **代码注释** - 保持详细的注释
- **错误处理** - 完善的错误处理
- **代码规范** - 遵循 Go 代码规范

## 📝 当前状态

- ✅ **数据结构基础实现** - 完成
- ✅ **存储系统** - 完成
- 🚧 **数据结构完善** - 进行中
- ⏳ **命令系统** - 待开发
- ⏳ **网络层** - 待开发

## 🎓 学习目标

通过这个项目，你将：
1. **深入理解 Redis** - 理解 Redis 的内部实现
2. **掌握数据结构** - 掌握各种高效数据结构
3. **提升编程能力** - 提升系统设计和实现能力
4. **准备面试** - 为 Redis 相关面试做好准备

---

**下一步行动**：建议先完善 Listpack 和 Skiplist 的实现，然后实现命令系统和网络层。

