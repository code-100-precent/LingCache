# Redis List 数据结构实现总结

## 编码方式

Redis List 有两种编码方式：

1. **OBJ_ENCODING_LISTPACK (11)** - 列表包，用于小列表
2. **OBJ_ENCODING_QUICKLIST (9)** - 快速列表，用于大列表（主要方式）

## Quicklist 结构（主要实现）

### 1. quicklistNode（节点）
```c
typedef struct quicklistNode {
    struct quicklistNode *prev;    // 前驱节点
    struct quicklistNode *next;    // 后继节点
    unsigned char *entry;          // 指向 listpack 数据
    size_t sz;                     // entry 大小（字节）
    unsigned int count : 16;       // listpack 中的元素数量
    unsigned int encoding : 2;      // RAW=1 或 LZF=2（压缩）
    unsigned int container : 2;    // PLAIN=1 或 PACKED=2（listpack）
    unsigned int recompress : 1;   // 是否临时解压
    unsigned int attempted_compress : 1;
    unsigned int dont_compress : 1;
    unsigned int extra : 9;
} quicklistNode;
```

### 2. quicklist（快速列表）
```c
typedef struct quicklist {
    quicklistNode *head;           // 头节点
    quicklistNode *tail;           // 尾节点
    unsigned long count;           // 所有 listpack 中的总元素数
    unsigned long len;             // quicklistNode 的数量
    size_t alloc_size;            // 总分配内存（字节）
    signed int fill : 16;         // 每个节点的填充因子
    unsigned int compress : 16;    // 压缩深度（两端不压缩的节点数）
    unsigned int bookmark_count: 4;
    quicklistBookmark bookmarks[];
} quicklist;
```

### 3. 内存布局

```
quicklist
├── head -> quicklistNode
│   ├── prev: NULL
│   ├── next -> next node
│   ├── entry -> listpack 数据
│   ├── sz: listpack 大小
│   └── count: listpack 中元素数
├── tail -> quicklistNode
│   └── ...
└── count: 总元素数
```

## Listpack 结构（小列表）

Listpack 是一个紧凑的列表格式，类似于 ziplist，但设计更简单：

```c
typedef struct {
    unsigned char *sval;  // 字符串值（如果是字符串）
    uint32_t slen;        // 字符串长度
    long long lval;       // 整数值（如果是整数）
} listpackEntry;
```

## 自动转换机制

1. **小列表**：使用 `OBJ_ENCODING_LISTPACK`
   - 单个 listpack 存储所有元素
   - 内存紧凑，适合小数据

2. **大列表**：转换为 `OBJ_ENCODING_QUICKLIST`
   - 当 listpack 超过 `list_max_listpack_size` 时转换
   - 使用 quicklist 存储多个 listpack 节点

3. **缩小转换**：quicklist 可能转回 listpack
   - 当 quicklist 只有一个节点且大小足够小时
   - 使用更严格的阈值避免频繁转换

## 关键操作

### Push 操作
```c
void listTypePush(robj *subject, robj *value, int where) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistPush(subject->ptr, value->ptr, sdslen(value->ptr), pos);
    } else if (subject->encoding == OBJ_ENCODING_LISTPACK) {
        subject->ptr = lpAppend(subject->ptr, value->ptr, sdslen(value->ptr));
    }
}
```

### Pop 操作
```c
robj *listTypePop(robj *subject, int where) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistPopCustom(subject->ptr, ql_where, ...);
    } else if (subject->encoding == OBJ_ENCODING_LISTPACK) {
        lpDelete(subject->ptr, p, NULL);
    }
}
```

