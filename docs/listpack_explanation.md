# Listpack 详解

## 什么是 Listpack？

**Listpack**（List Pack）是 Redis 设计的一个**紧凑的字符串列表序列化格式**，用于高效存储多个字符串或整数。

### 核心特点

1. **紧凑的二进制格式** - 所有数据连续存储在内存中
2. **支持字符串和整数** - 可以存储字符串或整数类型
3. **内存高效** - 比普通数组更节省内存
4. **顺序访问** - 适合顺序遍历，不适合随机访问

## Listpack vs Ziplist

Listpack 是 **Ziplist 的改进版本**：

| 特性 | Ziplist | Listpack |
|------|---------|----------|
| 设计复杂度 | 较复杂 | 更简单 |
| 前向遍历 | 需要 prevlen | 不需要 |
| 级联更新 | 可能发生 | 避免 |
| 内存布局 | 双向链表式 | 单向紧凑 |

**关键改进**：Listpack 避免了 ziplist 的"级联更新"问题（当一个元素变大时，需要更新后续所有元素的 prevlen）。

## 内存布局

### 整体结构

```
┌─────────────────────────────────────────┐
│         Listpack 内存布局                 │
├─────────────────────────────────────────┤
│  Header (6 bytes)                        │
│  ├─ Total Bytes (4 bytes, uint32)       │
│  └─ Num Elements (2 bytes, uint16)      │
├─────────────────────────────────────────┤
│  Entry 1                                 │
│  ├─ encoding (1 byte)                   │
│  ├─ element-len (变长)                  │
│  └─ element-data (变长)                 │
├─────────────────────────────────────────┤
│  Entry 2                                 │
│  └─ ...                                  │
├─────────────────────────────────────────┤
│  ...                                     │
├─────────────────────────────────────────┤
│  Entry N                                 │
│  └─ ...                                  │
├─────────────────────────────────────────┤
│  EOF (1 byte, 0xFF)                      │
└─────────────────────────────────────────┘
```

### 头部结构（6 字节）

```c
#define LP_HDR_SIZE 6

// 头部布局：
// [0-3]: Total Bytes (uint32, 小端序)
// [4-5]: Num Elements (uint16, 小端序)
```

### 条目（Entry）结构

每个条目包含：
1. **Encoding** - 编码类型（1 字节）
2. **Element Length** - 元素长度（变长编码）
3. **Element Data** - 实际数据

## 编码类型

### 整数编码

```c
// 7位无符号整数 (0-127)
LP_ENCODING_7BIT_UINT = 0x00
// 格式: [0xxxxxxx] [value]
// 示例: 0x05 表示整数 5

// 13位有符号整数
LP_ENCODING_13BIT_INT = 0xC0
// 格式: [110xxxxx] [xxxx xxxx]
// 范围: -8192 到 8191

// 16位有符号整数
LP_ENCODING_16BIT_INT = 0xF1
// 格式: [11110001] [xxxx xxxx] [xxxx xxxx]

// 24位有符号整数
LP_ENCODING_24BIT_INT = 0xF2
// 格式: [11110010] [xxxx xxxx] [xxxx xxxx] [xxxx xxxx]

// 32位有符号整数
LP_ENCODING_32BIT_INT = 0xF3
// 格式: [11110011] [4 bytes]

// 64位有符号整数
LP_ENCODING_64BIT_INT = 0xF4
// 格式: [11110100] [8 bytes]
```

### 字符串编码

```c
// 6位字符串长度 (0-63 字节)
LP_ENCODING_6BIT_STR = 0x80
// 格式: [10xxxxxx] [string data]
// 长度在第一个字节的低6位

// 12位字符串长度 (0-4095 字节)
LP_ENCODING_12BIT_STR = 0xE0
// 格式: [1110xxxx] [xxxx xxxx] [string data]
// 长度在第一个字节的低4位 + 第二个字节

// 32位字符串长度 (最大 4GB)
LP_ENCODING_32BIT_STR = 0xF0
// 格式: [11110000] [4 bytes length] [string data]
```

## 关键操作

### 1. 创建 Listpack

```c
unsigned char *lpNew(size_t capacity);
// 创建一个新的 listpack，预分配 capacity 字节
```

### 2. 添加元素

```c
// 在末尾添加字符串
unsigned char *lpAppend(unsigned char *lp, unsigned char *s, uint32_t slen);

// 在末尾添加整数
unsigned char *lpAppendInteger(unsigned char *lp, long long lval);

// 在开头添加
unsigned char *lpPrepend(unsigned char *lp, unsigned char *s, uint32_t slen);
```

### 3. 插入/删除

```c
// 在指定位置插入
unsigned char *lpInsertString(unsigned char *lp, unsigned char *s, uint32_t slen,
                              unsigned char *p, int where, unsigned char **newp);

// 删除元素
unsigned char *lpDelete(unsigned char *lp, unsigned char *p, unsigned char **newp);
```

### 4. 遍历

```c
// 获取第一个元素
unsigned char *lpFirst(unsigned char *lp);

// 获取下一个元素
unsigned char *lpNext(unsigned char *lp, unsigned char *p);

// 获取最后一个元素
unsigned char *lpLast(unsigned char *lp);

// 获取前一个元素
unsigned char *lpPrev(unsigned char *lp, unsigned char *p);
```

### 5. 读取元素

```c
// 获取元素值
unsigned char *lpGet(unsigned char *p, int64_t *count, unsigned char *intbuf);

// 获取元素值（区分字符串和整数）
unsigned char *lpGetValue(unsigned char *p, unsigned int *slen, long long *lval);
```

## 使用场景

### 1. Redis List（小列表）

当列表元素较少时，使用单个 listpack 存储：

```c
// 小列表：OBJ_ENCODING_LISTPACK
robj *o = createObject(OBJ_LIST, lpNew(0));
lpAppend(o->ptr, "hello", 5);
lpAppend(o->ptr, "world", 5);
```

### 2. Quicklist 节点

Quicklist 的每个节点包含一个 listpack：

```c
quicklistNode *node = quicklistCreateNode();
node->entry = lpNew(0);  // listpack 数据
node->container = QUICKLIST_NODE_CONTAINER_PACKED;
```

### 3. Hash 字段（小哈希）

小哈希表也使用 listpack 存储字段：

```c
// Hash: OBJ_ENCODING_LISTPACK
robj *o = createObject(OBJ_HASH, lpNew(0));
lpAppend(o->ptr, "name", 4);
lpAppend(o->ptr, "redis", 5);
```

## 优势

1. **内存紧凑** - 连续存储，减少内存碎片
2. **避免级联更新** - 相比 ziplist，插入/删除更高效
3. **支持整数优化** - 小整数直接编码，节省空间
4. **简单设计** - 代码更易维护

## 限制

1. **顺序访问** - 只能顺序遍历，不支持随机访问
2. **插入/删除成本** - 需要移动后续数据
3. **大小限制** - 最大 1GB（安全限制）

## 示例

### 创建一个包含 3 个元素的 listpack

```
Header: [总长度] [元素数=3]
Entry 1: [编码] [长度] "hello"
Entry 2: [编码] [长度] "world"  
Entry 3: [编码] [长度] 123
EOF: 0xFF
```

### 内存示例（简化）

```
[06 00 00 00] [03 00] [80 05] [68 65 6c 6c 6f] [80 05] [77 6f 72 6c 64] [00 7b] [ff]
  ↑总长度6字节  ↑3个元素  ↑6bit str len=5  ↑"hello"  ↑6bit str len=5  ↑"world" ↑7bit int=123 ↑EOF
```

## 总结

Listpack 是 Redis 中用于存储小列表/哈希的紧凑格式，特点是：
- **紧凑**：连续内存，节省空间
- **简单**：相比 ziplist 设计更简洁
- **高效**：避免级联更新问题
- **灵活**：支持字符串和整数混合存储

在 Redis List 中，小列表直接用 listpack，大列表用 quicklist（多个 listpack 节点）。

