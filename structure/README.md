# Redis 数据结构 Golang 实现

本项目用 Golang 完整实现了 Redis 的五种核心数据结构，包括详细的注释、原理说明和面试题。

## 📁 文件结构

```
structure/
├── encoding.go          # 编码类型定义
├── str.go              # String (SDS) 实现
├── list.go             # List (Quicklist + Listpack) 实现
├── set.go              # Set (Intset + Hashtable) 实现
├── zset.go             # Sorted Set (Skiplist + Dict) 实现
├── hash.go             # Hash (Listpack + Dict) 实现
├── str_test.go         # String 测试
├── list_test.go        # List 测试
├── interview_questions.md  # 面试题总结
└── README.md           # 本文件
```

## 🎯 实现的数据结构

### 1. String (SDS - Simple Dynamic String)

**文件**: `str.go`

**编码方式**:
- SDS_TYPE_8/16/32/64: 根据字符串长度选择不同的 header 类型

**核心特性**:
- ✅ O(1) 时间复杂度获取长度
- ✅ 二进制安全（可以存储任意二进制数据）
- ✅ 空间预分配（减少内存重分配）
- ✅ 惰性空间释放（缩短时不立即释放内存）
- ✅ 多种 header 类型优化内存使用

**主要 API**:
```go
NewSDS(init string) SDS
SDSCat(s SDS, t string) SDS
SDSCmp(s1, s2 SDS) int
SDSTrim(s SDS, cutset string) SDS
```

### 2. List (Quicklist + Listpack)

**文件**: `list.go`

**编码方式**:
- OBJ_ENCODING_LISTPACK: 小列表（< 8KB 或 < 512 元素）
- OBJ_ENCODING_QUICKLIST: 大列表（双向链表 + listpack）

**核心特性**:
- ✅ 小列表使用 listpack（内存紧凑）
- ✅ 大列表使用 quicklist（支持压缩）
- ✅ 自动编码转换
- ✅ 支持头部和尾部操作

**主要 API**:
```go
NewList() *RedisList
Push(value []byte, where int)  // where: 0=HEAD, 1=TAIL
Pop(where int) ([]byte, error)
Range(start, end int) ([][]byte, error)
```

### 3. Set (Intset + Hashtable)

**文件**: `set.go`

**编码方式**:
- OBJ_ENCODING_INTSET: 整数集合（所有元素都是整数）
- OBJ_ENCODING_HT: 哈希表（包含非整数或元素过多）

**核心特性**:
- ✅ 小整数集合使用 intset（有序、紧凑）
- ✅ 大集合使用 hashtable（O(1) 查找）
- ✅ 自动编码转换（intset → hashtable）
- ✅ 支持集合运算（交集、并集、差集）

**主要 API**:
```go
NewSet() *RedisSet
Add(member []byte) error
Remove(member []byte) error
IsMember(member []byte) bool
Inter(others ...*RedisSet) *RedisSet
Union(others ...*RedisSet) *RedisSet
Diff(others ...*RedisSet) *RedisSet
```

### 4. Sorted Set (ZSet) - Skiplist + Dict

**文件**: `zset.go`

**编码方式**:
- OBJ_ENCODING_LISTPACK: 小有序集合（< 128 元素）
- OBJ_ENCODING_SKIPLIST: 大有序集合（跳表 + 字典）

**核心特性**:
- ✅ 小集合使用 listpack（内存紧凑）
- ✅ 大集合使用 skiplist + dict（范围查询 + O(1) 查找 score）
- ✅ 支持按 score 排序
- ✅ 支持范围查询（ZRANGE）

**主要 API**:
```go
NewZSet() *RedisZSet
Add(member []byte, score float64) error
Remove(member []byte) error
Score(member []byte) (float64, bool)
Rank(member []byte, reverse bool) (int, bool)
Range(start, end int, reverse bool) ([]ZSetEntry, error)
```

### 5. Hash (Listpack + Dict)

**文件**: `hash.go`

**编码方式**:
- OBJ_ENCODING_LISTPACK: 小哈希表（< 512 元素，字段/值 < 64 字节）
- OBJ_ENCODING_HT: 大哈希表（哈希表）

**核心特性**:
- ✅ 小哈希表使用 listpack（内存紧凑）
- ✅ 大哈希表使用 dict（O(1) 查找）
- ✅ 自动编码转换
- ✅ 支持字段操作（HGET、HSET、HDEL 等）

**主要 API**:
```go
NewHash() *RedisHash
Set(field, value []byte) error
Get(field []byte) ([]byte, bool)
Del(field []byte) error
Exists(field []byte) bool
IncrBy(field []byte, increment int64) (int64, error)
MSet(fields, values [][]byte) error
MGet(fields [][]byte) [][]byte
```

## 📚 详细文档

每个数据结构文件都包含：
1. **核心原理说明**：数据结构的设计思想和优势
2. **内存布局图**：可视化展示数据结构的内存组织
3. **编码转换策略**：何时从一种编码转换到另一种
4. **面试题**：常见面试题和详细答案

## 🎓 面试题总结

完整的面试题总结请查看：[interview_questions.md](./interview_questions.md)

包含：
- 每种数据结构的 5 个核心面试题
- 通用问题（编码转换、内存优化等）
- 实战问题（如何选择数据结构、性能优化等）

## 🚀 使用示例

### String (SDS)
```go
s := NewSDS("hello")
s = SDSCat(s, " world")
println(string(sdsBytes(s))) // "hello world"
```

### List
```go
list := NewList()
list.Push([]byte("hello"), 1) // TAIL
list.Push([]byte("world"), 0) // HEAD
value, _ := list.Pop(0)
println(string(value)) // "world"
```

### Set
```go
set := NewSet()
set.Add([]byte("apple"))
set.Add([]byte("banana"))
println(set.IsMember([]byte("apple"))) // true
```

### ZSet
```go
zset := NewZSet()
zset.Add([]byte("alice"), 100.0)
zset.Add([]byte("bob"), 90.0)
score, _ := zset.Score([]byte("alice"))
println(score) // 100.0
```

### Hash
```go
hash := NewHash()
hash.Set([]byte("name"), []byte("Alice"))
hash.Set([]byte("age"), []byte("30"))
value, _ := hash.Get([]byte("name"))
println(string(value)) // "Alice"
```

## 🔍 实现特点

1. **完整实现**：每种数据结构都实现了核心功能
2. **详细注释**：每个函数都有详细的中文注释
3. **原理说明**：解释了为什么这样设计
4. **面试题**：包含常见面试题和答案
5. **编码转换**：实现了自动编码转换逻辑

## ⚠️ 注意事项

1. **简化实现**：某些复杂功能（如 LZF 压缩、完整的 listpack 序列化）做了简化
2. **内存管理**：Go 的 GC 会自动管理内存，某些手动内存管理逻辑已简化
3. **并发安全**：当前实现不是并发安全的（Redis 是单线程的）

## 📖 学习建议

1. **先理解原理**：阅读每个文件开头的原理说明
2. **查看代码**：理解数据结构的实现细节
3. **运行测试**：运行测试用例，观察行为
4. **阅读面试题**：掌握常见面试题的答案
5. **对比 Redis 源码**：可以对比 Redis 的 C 语言实现

## 🔗 参考资源

- [Redis 源码](https://github.com/redis/redis)
- [Redis 官方文档](https://redis.io/docs/)
- [面试题总结](./interview_questions.md)

## 📝 许可证

本项目仅用于学习和研究目的。

