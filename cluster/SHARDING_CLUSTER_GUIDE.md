# LingCache 分片集群使用教程

## 概述

LingCache 分片集群（Sharding Cluster）基于 Redis Cluster 协议实现，通过哈希槽（Hash Slot）将数据分布到多个节点，实现水平扩展。

### 核心特性

- ✅ **自动分片**：16384 个哈希槽自动分配到多个节点
- ✅ **数据路由**：根据键的哈希值自动路由到正确的节点
- ✅ **MOVED 重定向**：槽迁移时自动重定向客户端
- ✅ **高可用性**：支持主从复制和故障转移
- ✅ **水平扩展**：动态添加/删除节点

---

## 快速开始

### 1. 启动 3 节点集群

#### 节点 1（端口 7000）

```bash
# 创建节点目录
mkdir -p cluster/node1 && cd cluster/node1

# 创建配置文件
cat > .env << EOF
REDIS_ADDR=:7000
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7000
REDIS_CLUSTER_NODE_ID=node1
REDIS_AOF_ENABLED=true
EOF

# 启动节点
../../lingcache-server -addr :7000
```

#### 节点 2（端口 7001）

```bash
mkdir -p cluster/node2 && cd cluster/node2

cat > .env << EOF
REDIS_ADDR=:7001
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7001
REDIS_CLUSTER_NODE_ID=node2
REDIS_AOF_ENABLED=true
EOF

../../lingcache-server -addr :7001
```

#### 节点 3（端口 7002）

```bash
mkdir -p cluster/node3 && cd cluster/node3

cat > .env << EOF
REDIS_ADDR=:7002
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7002
REDIS_CLUSTER_NODE_ID=node3
REDIS_AOF_ENABLED=true
EOF

../../lingcache-server -addr :7002
```

### 2. 节点握手

连接到任意节点（如节点1）：

```bash
node ../../client.js
```

执行节点握手：

```bash
# 让节点1认识节点2
CLUSTER MEET 127.0.0.1 7001

# 让节点1认识节点3
CLUSTER MEET 127.0.0.1 7002
```

### 3. 分配槽

在每个节点上分配槽：

**节点1**（连接到 :7000）：
```bash
CLUSTER ADDSLOTS 0 1 2 3 ... 5460
```

**节点2**（连接到 :7001）：
```bash
CLUSTER ADDSLOTS 5461 5462 5463 ... 10922
```

**节点3**（连接到 :7002）：
```bash
CLUSTER ADDSLOTS 10923 10924 10925 ... 16383
```

**提示**：可以使用脚本批量分配槽（见下方脚本）。

### 4. 验证集群

```bash
# 查看集群状态
CLUSTER INFO

# 查看节点信息
CLUSTER NODES

# 查看槽分配
CLUSTER SLOTS
```

---

## 槽分配脚本

### 自动分配脚本

创建 `assign_slots.sh`：

```bash
#!/bin/bash

# 节点配置
NODES=(
    "node1:7000:0:5460"
    "node2:7001:5461:10922"
    "node3:7002:10923:16383"
)

for node_config in "${NODES[@]}"; do
    IFS=':' read -r node_id port start_slot end_slot <<< "$node_config"
    
    echo "Assigning slots $start_slot-$end_slot to $node_id on port $port"
    
    # 生成槽列表
    slots=""
    for ((slot=start_slot; slot<=end_slot; slot++)); do
        slots="$slots $slot"
    done
    
    # 发送命令（需要实现客户端连接）
    echo "CLUSTER ADDSLOTS $slots" | nc localhost $port
done
```

### 手动分配示例

```bash
# 节点1：分配槽 0-5460
for i in {0..5460}; do
    echo "CLUSTER ADDSLOTS $i" | nc localhost 7000
done

# 节点2：分配槽 5461-10922
for i in {5461..10922}; do
    echo "CLUSTER ADDSLOTS $i" | nc localhost 7001
done

# 节点3：分配槽 10923-16383
for i in {10923..16383}; do
    echo "CLUSTER ADDSLOTS $i" | nc localhost 7002
done
```

---

## 使用分片集群

### 1. 数据操作

连接到任意节点，数据会自动路由到正确的节点：

```bash
# 连接到节点1
node client.js

# 设置数据（自动路由到正确的节点）
SET user:1000 "Alice"
SET user:2000 "Bob"
SET user:3000 "Charlie"

# 获取数据（自动路由）
GET user:1000
GET user:2000
GET user:3000
```

### 2. 查看路由信息

```bash
# 查看键对应的槽
# 槽号 = CRC16(key) % 16384

# 查看槽分配
CLUSTER SLOTS

# 查看节点信息
CLUSTER NODES
```

### 3. 处理 MOVED 重定向

当键不在当前节点时，服务器会返回 MOVED 重定向：

```
-MOVED 1234 127.0.0.1:7001
```

客户端应该：
1. 解析 MOVED 响应
2. 连接到新节点
3. 重新发送请求
4. 更新本地路由缓存

**注意**：当前客户端实现需要手动处理重定向，生产环境建议使用支持集群的客户端库。

---

## 哈希标签（Hash Tag）

### 什么是哈希标签？

使用 `{}` 指定哈希标签，只有标签内的内容参与哈希计算，可以将相关键路由到同一节点。

### 使用示例

```bash
# 不使用标签（可能在不同节点）
SET user:1000:profile "data1"
SET user:1000:settings "data2"

# 使用标签（在同一节点）
SET {user:1000}:profile "data1"
SET {user:1000}:settings "data2"
```

### 应用场景

1. **多键操作**：需要同时操作多个相关键
2. **事务**：MULTI/EXEC 只能在同一节点执行
3. **Lua 脚本**：脚本在单个节点执行

---

## 集群管理

### 查看集群状态

```bash
# 集群信息
CLUSTER INFO

# 输出示例：
# cluster_state:ok
# cluster_slots_assigned:16384
# cluster_slots_ok:16384
# cluster_known_nodes:3
# cluster_size:3
```

### 查看节点信息

```bash
CLUSTER NODES

# 输出示例：
# node1 127.0.0.1:7000@0 master - 0 0 0 connected 0-5460
# node2 127.0.0.1:7001@0 master - 0 0 0 connected 5461-10922
# node3 127.0.0.1:7002@0 master - 0 0 0 connected 10923-16383
```

### 查看槽分配

```bash
CLUSTER SLOTS

# 输出示例：
# 1) 1) (integer) 0
#    2) (integer) 5460
#    3) 1) "127.0.0.1"
#       2) (integer) 7000
#       3) "node1"
```

---

## 槽迁移（Resharding）

### 迁移单个槽

```bash
# 1. 在目标节点准备接收
CLUSTER SETSLOT 1000 IMPORTING node1 node2

# 2. 在源节点准备迁移
CLUSTER SETSLOT 1000 MIGRATING node2 node1

# 3. 迁移数据（需要实现 MIGRATE 命令）
# MIGRATE 127.0.0.1 7001 "" 0 5000 KEYS key1 key2

# 4. 更新所有节点
CLUSTER SETSLOT 1000 NODE node2
```

### 批量迁移

使用脚本批量迁移多个槽：

```bash
#!/bin/bash
SOURCE_NODE="node1"
TARGET_NODE="node2"
SLOTS="1000 1001 1002 1003 1004"

for slot in $SLOTS; do
    echo "Migrating slot $slot from $SOURCE_NODE to $TARGET_NODE"
    # 执行迁移步骤...
done
```

---

## 故障转移

### 自动故障转移

当主节点故障时：
1. 从节点检测到主节点故障
2. 从节点自动提升为主节点
3. 接管原主节点的槽
4. 更新集群拓扑

### 手动故障转移

```bash
# 在从节点上执行
CLUSTER FAILOVER
```

---

## 最佳实践

### 1. 节点规划

- **最少 3 个主节点**：确保集群可用性
- **主从比例 1:1**：每个主节点至少一个从节点
- **奇数个主节点**：避免投票平局

### 2. 槽分配

- **均匀分配**：尽量让每个节点分配的槽数量相近
- **考虑负载**：根据节点性能调整槽分配
- **预留容量**：为未来扩展预留槽空间

### 3. 键命名

- **使用哈希标签**：将相关键路由到同一节点
- **避免跨槽操作**：减少跨节点操作
- **合理分片**：根据数据特点调整分片策略

### 4. 客户端实现

- **支持 MOVED 重定向**：自动重定向到正确节点
- **维护路由缓存**：减少重定向次数
- **连接池管理**：为每个节点维护连接池

---

## 常见问题

### Q1: 如何计算键的槽号？

**A:** 使用 CRC16 算法：
```
slot = CRC16(key) % 16384
```

如果使用哈希标签：
```
tag = extract_hash_tag(key)  // 提取 {} 中的内容
slot = CRC16(tag) % 16384
```

### Q2: 为什么收到 MOVED 重定向？

**A:** 可能的原因：
- 槽已迁移到其他节点
- 节点故障转移后槽重新分配
- 客户端路由缓存过期

**处理**：更新路由缓存，重定向到新节点。

### Q3: 支持跨槽操作吗？

**A:** 不支持跨槽的批量操作，但可以使用：
- **哈希标签**：将相关键路由到同一节点
- **Lua 脚本**：脚本在单个节点执行
- **客户端分片**：客户端将操作拆分到不同节点

### Q4: 如何添加新节点？

**A:** 
1. 启动新节点
2. 使用 `CLUSTER MEET` 加入集群
3. 迁移部分槽到新节点
4. 配置从节点（可选）

### Q5: 如何移除节点？

**A:**
1. 如果是主节点，先迁移所有槽
2. 如果是从节点，直接移除
3. 在所有节点上执行 `CLUSTER FORGET <node-id>`

---

## 示例场景

### 场景 1: 3 节点集群

```bash
# 启动节点
./lingcache-server -addr :7000  # node1
./lingcache-server -addr :7001  # node2
./lingcache-server -addr :7002  # node3

# 节点握手
CLUSTER MEET 127.0.0.1 7001
CLUSTER MEET 127.0.0.1 7002

# 分配槽
# node1: 0-5460
# node2: 5461-10922
# node3: 10923-16383
```

### 场景 2: 使用哈希标签

```bash
# 将用户相关数据路由到同一节点
SET {user:1000}:profile "Alice's profile"
SET {user:1000}:settings "Alice's settings"
SET {user:1000}:friends "Alice's friends"

# 这些键都在同一个槽，可以在同一节点执行事务
MULTI
GET {user:1000}:profile
GET {user:1000}:settings
EXEC
```

### 场景 3: 扩容集群

```bash
# 1. 启动新节点
./lingcache-server -addr :7003  # node4

# 2. 加入集群
CLUSTER MEET 127.0.0.1 7003

# 3. 迁移部分槽到新节点
# ... 执行槽迁移 ...
```

---

## 与主从复制的区别

| 特性 | 分片集群 | 主从复制 |
|------|---------|---------|
| **数据分布** | 分片到多个节点 | 所有数据在一个主节点 |
| **扩展性** | 水平扩展 | 垂直扩展 |
| **适用场景** | 大数据量 | 小数据量 |
| **复杂度** | 较高 | 较低 |
| **一致性** | 最终一致性 | 最终一致性 |

**详细对比请查看**: [主从复制 vs 分片集群对比指南](../REPLICATION_VS_CLUSTER.md)

---

## 相关文档

- [集群使用指南](./USAGE_GUIDE.md) - 完整的集群使用文档
- [快速开始](./QUICK_START.md) - 快速启动指南
- [实现总结](./IMPLEMENTATION_SUMMARY.md) - 功能实现详情

---

**最后更新**: 2024-12-20
**版本**: 1.0.0

