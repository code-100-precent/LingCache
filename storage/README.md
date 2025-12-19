# Redis 存储系统实现

本目录实现了类似 Redis 的存储系统，包括数据库管理、键值存储、过期机制等核心功能。

## 📁 文件结构

```
storage/
├── object.go      # Redis 对象系统 (robj)
├── db.go          # 数据库实现 (redisDb)
├── server.go      # 服务器实现
├── errors.go      # 错误定义
├── example.go     # 使用示例
├── main.go        # 主程序入口
└── README.md      # 本文件
```

## 🎯 核心功能

### 1. 对象系统 (object.go)

**RedisObject** 是统一的对象表示：
- **Type**: 对象类型（STRING、LIST、SET、ZSET、HASH）
- **Encoding**: 编码方式（决定底层数据结构）
- **Ptr**: 指向实际数据的指针
- **RefCount**: 引用计数（用于内存管理）

**主要 API**:
```go
NewStringObject(value []byte) *RedisObject
NewListObject() *RedisObject
NewSetObject() *RedisObject
NewZSetObject() *RedisObject
NewHashObject() *RedisObject
```

### 2. 数据库系统 (db.go)

**RedisDb** 管理键值对存储：
- **keys**: 键值对存储（map[string]*RedisObject）
- **expires**: 过期时间存储（map[string]int64）
- **并发安全**: 使用读写锁保证线程安全

**主要 API**:
```go
Set(key string, obj *RedisObject)
Get(key string) (*RedisObject, error)
Del(key string) bool
Exists(key string) bool
Type(key string) (string, error)
TTL(key string) (int64, error)
Expire(key string, seconds int64) bool
Keys(pattern string) []string
DBSize() int
FlushDB()
```

### 3. 服务器系统 (server.go)

**RedisServer** 管理多个数据库：
- **dbs**: 数据库数组（默认 16 个）
- **currentDb**: 当前选中的数据库

**主要 API**:
```go
NewRedisServer(dbnum int) *RedisServer
SelectDb(dbIndex int) error
GetCurrentDb() *RedisDb
GetDb(dbIndex int) (*RedisDb, error)
FlushAll()
```

## 🚀 使用示例

### 基本使用

```go
package main

import (
    "fmt"
    "github.com/code-100-precent/LingCache/storage"
)

func main() {
    // 创建 Redis 服务器（16 个数据库）
    server := storage.NewRedisServer(16)
    
    // 获取当前数据库（默认是数据库 0）
    db := server.GetCurrentDb()
    
    // ========== String 操作 ==========
    // SET key value
    key := "name"
    value := storage.NewStringObject([]byte("Alice"))
    db.Set(key, value)
    
    // GET key
    obj, err := db.Get(key)
    if err == nil {
        val, _ := obj.GetStringValue()
        fmt.Printf("GET %s = %s\n", key, string(val))
    }
    
    // ========== List 操作 ==========
    listKey := "mylist"
    listObj := storage.NewListObject()
    db.Set(listKey, listObj)
    
    list, _ := listObj.GetList()
    list.Push([]byte("world"), 0) // HEAD
    list.Push([]byte("hello"), 0) // HEAD
    
    // LRANGE list 0 -1
    values, _ := list.Range(0, -1)
    for _, v := range values {
        fmt.Println(string(v))
    }
    
    // ========== Set 操作 ==========
    setKey := "myset"
    setObj := storage.NewSetObject()
    db.Set(setKey, setObj)
    
    set, _ := setObj.GetSet()
    set.Add([]byte("apple"))
    set.Add([]byte("banana"))
    
    fmt.Printf("SCARD %s = %d\n", setKey, set.Card())
    fmt.Printf("SISMEMBER %s apple = %v\n", setKey, set.IsMember([]byte("apple")))
    
    // ========== ZSet 操作 ==========
    zsetKey := "myzset"
    zsetObj := storage.NewZSetObject()
    db.Set(zsetKey, zsetObj)
    
    zset, _ := zsetObj.GetZSet()
    zset.Add([]byte("alice"), 100.0)
    zset.Add([]byte("bob"), 90.0)
    
    score, _ := zset.Score([]byte("alice"))
    fmt.Printf("ZSCORE %s alice = %.1f\n", zsetKey, score)
    
    // ========== Hash 操作 ==========
    hashKey := "user:1"
    hashObj := storage.NewHashObject()
    db.Set(hashKey, hashObj)
    
    hash, _ := hashObj.GetHash()
    hash.Set([]byte("name"), []byte("Alice"))
    hash.Set([]byte("age"), []byte("30"))
    
    age, _ := hash.Get([]byte("age"))
    fmt.Printf("HGET %s age = %s\n", hashKey, string(age))
    
    // ========== 过期时间操作 ==========
    expireKey := "temp_key"
    tempObj := storage.NewStringObject([]byte("temporary"))
    db.Set(expireKey, tempObj)
    db.Expire(expireKey, 5) // 5 秒后过期
    
    ttl, _ := db.TTL(expireKey)
    fmt.Printf("TTL %s = %d\n", expireKey, ttl)
}
```

### 运行完整示例

```bash
cd storage
go run main.go example.go
```

## 📚 数据结构支持

存储系统支持所有五种 Redis 数据结构：

1. **String**: 字符串对象
2. **List**: 列表对象（支持 LPUSH、RPUSH、LRANGE 等）
3. **Set**: 集合对象（支持 SADD、SISMEMBER、SINTER 等）
4. **ZSet**: 有序集合对象（支持 ZADD、ZSCORE、ZRANGE 等）
5. **Hash**: 哈希对象（支持 HSET、HGET、HGETALL 等）

## 🔧 特性

### 1. 多数据库支持
- 默认支持 16 个数据库（可配置）
- 使用 `SELECT` 命令切换数据库
- 每个数据库独立存储，互不干扰

### 2. 过期机制
- 支持 `EXPIRE` 设置过期时间（秒）
- 支持 `EXPIREAT` 设置过期时间（Unix 时间戳）
- 支持 `TTL` 查询剩余生存时间
- 支持 `PERSIST` 移除过期时间
- 自动清理过期键

### 3. 并发安全
- 使用读写锁（`sync.RWMutex`）保证并发安全
- 支持多 goroutine 并发访问

### 4. 引用计数
- 对象使用引用计数管理内存
- 自动处理对象生命周期

## 🎓 设计原理

### 对象系统
Redis 使用统一的对象系统（robj）来表示所有数据类型，这样可以：
- 统一内存管理
- 支持多态操作
- 简化代码实现

### 数据库系统
每个数据库使用哈希表存储键值对：
- **keys**: 存储 key -> RedisObject 的映射
- **expires**: 存储 key -> expire time 的映射

### 过期机制
使用单独的哈希表存储过期时间，定期清理过期键：
- 访问时检查是否过期
- 后台任务定期清理

## ⚠️ 注意事项

1. **内存管理**: Go 的 GC 会自动管理内存，引用计数主要用于逻辑管理
2. **持久化**: 当前实现不包含持久化功能（RDB/AOF）
3. **网络协议**: 当前实现不包含网络协议（RESP）
4. **命令处理**: 当前实现提供底层 API，不包含命令解析

## 🔗 相关文档

- [数据结构实现](../structure/README.md)
- [面试题总结](../structure/interview_questions.md)

## 📝 后续改进

1. **完善简化实现**: 完善 listpack、skiplist 等数据结构的完整实现
2. **持久化**: 实现 RDB 和 AOF 持久化
3. **网络协议**: 实现 RESP 协议支持
4. **命令系统**: 实现完整的命令处理系统
5. **事务支持**: 实现 MULTI/EXEC 事务
6. **发布订阅**: 实现 PUB/SUB 功能

