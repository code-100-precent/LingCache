# LingCache 集群使用指南

## 目录

1. [概述](#概述)
2. [快速开始](#快速开始)
3. [集群配置](#集群配置)
4. [启动集群](#启动集群)
5. [集群操作](#集群操作)
6. [数据迁移](#数据迁移)
7. [故障转移](#故障转移)
8. [监控和维护](#监控和维护)
9. [最佳实践](#最佳实践)
10. [常见问题](#常见问题)

---

## 概述

LingCache 集群模式基于 Redis Cluster 协议实现，提供：

- ✅ **自动分片**：16384 个哈希槽自动分配到多个节点
- ✅ **高可用性**：主从复制 + 自动故障转移
- ✅ **水平扩展**：动态添加/删除节点
- ✅ **数据迁移**：在线槽迁移，无需停机
- ✅ **客户端重定向**：自动路由到正确的节点

### 架构特点

- **无中心化**：所有节点地位平等，通过 Gossip 协议通信
- **哈希槽分片**：使用 CRC16 算法计算键的槽号
- **主从复制**：每个主节点可以有多个从节点
- **自动故障转移**：主节点故障时，从节点自动提升

---

## 快速开始

### 1. 准备环境

确保已安装 Go 1.16+ 并编译项目：

```bash
cd /path/to/LingCache
go build -o lingcache-server ./cmd/server
```

### 2. 创建配置文件

创建 `.env` 文件配置集群参数：

```bash
# 服务器配置
REDIS_ADDR=:6379
REDIS_DB_NUM=16

# 集群配置
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7000
REDIS_CLUSTER_NODE_ID=node1

# 持久化配置
REDIS_AOF_ENABLED=true
REDIS_AOF_FILENAME=appendonly.aof
REDIS_RDB_ENABLED=true
REDIS_RDB_FILENAME=dump.rdb
```

### 3. 启动第一个节点

```bash
./lingcache-server -addr :7000
```

输出示例：
```
LingCache server started on :7000
Database number: 16
RDB enabled: true
AOF enabled: true
Cluster mode: enabled (port: 7000)
```

---

## 集群配置

### 环境变量配置

| 变量名 | 说明 | 默认值 | 示例 |
|--------|------|--------|------|
| `REDIS_CLUSTER_ENABLED` | 启用集群模式 | `false` | `true` |
| `REDIS_CLUSTER_PORT` | 集群通信端口 | `7000` | `7000` |
| `REDIS_CLUSTER_NODE_ID` | 节点唯一标识 | `""` | `node1` |
| `REDIS_ADDR` | 客户端连接地址 | `:6379` | `:7000` |
| `REDIS_DB_NUM` | 数据库数量 | `16` | `16` |

### 节点命名规范

建议使用有意义的节点 ID：
- `node1`, `node2`, `node3` - 简单命名
- `master-1`, `master-2` - 主节点命名
- `replica-1-1`, `replica-1-2` - 从节点命名

---

## 启动集群

### 单机多节点部署（测试环境）

在同一台机器上启动多个节点，使用不同端口：

#### 节点 1（主节点）
```bash
# 创建节点1目录
mkdir -p cluster/node1
cd cluster/node1

# 创建配置文件
cat > .env << EOF
REDIS_ADDR=:7000
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7000
REDIS_CLUSTER_NODE_ID=node1
REDIS_AOF_ENABLED=true
REDIS_AOF_FILENAME=appendonly.aof
EOF

# 启动节点
../../lingcache-server -addr :7000
```

#### 节点 2（主节点）
```bash
mkdir -p cluster/node2
cd cluster/node2

cat > .env << EOF
REDIS_ADDR=:7001
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7001
REDIS_CLUSTER_NODE_ID=node2
REDIS_AOF_ENABLED=true
REDIS_AOF_FILENAME=appendonly.aof
EOF

../../lingcache-server -addr :7001
```

#### 节点 3（主节点）
```bash
mkdir -p cluster/node3
cd cluster/node3

cat > .env << EOF
REDIS_ADDR=:7002
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7002
REDIS_CLUSTER_NODE_ID=node3
REDIS_AOF_ENABLED=true
REDIS_AOF_FILENAME=appendonly.aof
EOF

../../lingcache-server -addr :7002
```

### 分布式部署（生产环境）

在不同机器上部署节点：

#### 机器 1 (192.168.1.10)
```bash
# .env
REDIS_ADDR=:7000
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7000
REDIS_CLUSTER_NODE_ID=node1
```

#### 机器 2 (192.168.1.11)
```bash
# .env
REDIS_ADDR=:7000
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7000
REDIS_CLUSTER_NODE_ID=node2
```

#### 机器 3 (192.168.1.12)
```bash
# .env
REDIS_ADDR=:7000
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7000
REDIS_CLUSTER_NODE_ID=node3
```

---

## 集群操作

### 1. 节点握手（MEET）

启动所有节点后，需要让它们互相认识。连接到任意节点执行：

```bash
# 使用客户端连接到节点1
node client.js

# 让节点1认识节点2
CLUSTER MEET 127.0.0.1 7001

# 让节点1认识节点3
CLUSTER MEET 127.0.0.1 7002
```

**注意**：只需要在一个节点上执行 MEET，集群会自动传播拓扑信息。

### 2. 查看集群信息

```bash
# 查看集群状态
CLUSTER INFO

# 输出示例：
# cluster_state:ok
# cluster_slots_assigned:16384
# cluster_slots_ok:16384
# cluster_known_nodes:3
```

### 3. 查看节点信息

```bash
# 查看所有节点
CLUSTER NODES

# 输出示例：
# node1 127.0.0.1:7000@17000 master - 0 1234567890 1234567891 0 connected 0-5460
# node2 127.0.0.1:7001@17001 master - 0 1234567890 1234567891 0 connected 5461-10922
# node3 127.0.0.1:7002@17002 master - 0 1234567890 1234567891 0 connected 10923-16383
```

### 4. 查看槽分配

```bash
# 查看槽分配信息
CLUSTER SLOTS

# 输出示例：
# 1) 1) (integer) 0
#    2) (integer) 5460
#    3) 1) "127.0.0.1"
#       2) (integer) 7000
#       3) "node1"
```

### 5. 手动分配槽（可选）

默认情况下，槽会自动分配。如果需要手动分配：

```bash
# 将槽 0-5460 分配给 node1
CLUSTER ADDSLOTS 0 1 2 ... 5460

# 将槽 5461-10922 分配给 node2
CLUSTER ADDSLOTS 5461 5462 ... 10922

# 将槽 10923-16383 分配给 node3
CLUSTER ADDSLOTS 10923 10924 ... 16383
```

---

## 数据迁移

### 槽迁移（Resharding）

当需要重新分配槽时，可以使用槽迁移功能：

#### 1. 开始迁移

```bash
# 将槽 1000 从 node1 迁移到 node2
CLUSTER SETSLOT 1000 MIGRATING node2
CLUSTER SETSLOT 1000 IMPORTING node1 node2
```

#### 2. 迁移数据

```bash
# 迁移槽中的所有键
MIGRATE 127.0.0.1 7001 "" 0 5000 KEYS key1 key2 key3
```

#### 3. 完成迁移

```bash
# 在所有节点上更新槽分配
CLUSTER SETSLOT 1000 NODE node2
```

### 批量迁移工具

可以使用脚本批量迁移多个槽：

```bash
#!/bin/bash
# migrate_slots.sh

SOURCE_NODE="node1"
TARGET_NODE="node2"
SLOTS="1000 1001 1002 1003 1004"

for slot in $SLOTS; do
    echo "Migrating slot $slot..."
    # 执行迁移命令
    # CLUSTER SETSLOT $slot MIGRATING $TARGET_NODE
    # ... 迁移数据 ...
    # CLUSTER SETSLOT $slot NODE $TARGET_NODE
done
```

---

## 故障转移

### 自动故障转移

当主节点故障时，集群会自动：

1. **检测故障**：其他节点通过心跳检测到主节点故障
2. **投票确认**：主节点投票确认故障
3. **选举新主**：从节点中选举新的主节点
4. **槽转移**：将原主节点的槽转移到新主节点
5. **更新拓扑**：通知所有节点更新集群拓扑

### 手动故障转移

如果需要手动触发故障转移（如维护）：

```bash
# 在从节点上执行，提升为主节点
CLUSTER FAILOVER
```

### 查看故障转移状态

```bash
# 查看集群状态
CLUSTER INFO

# 查看节点状态
CLUSTER NODES
```

---

## 监控和维护

### 1. 集群健康检查

```bash
# 查看集群信息
CLUSTER INFO

# 关键指标：
# - cluster_state:ok - 集群状态正常
# - cluster_slots_assigned:16384 - 所有槽已分配
# - cluster_slots_ok:16384 - 所有槽正常
# - cluster_known_nodes:3 - 已知节点数
```

### 2. 节点监控

```bash
# 查看节点详细信息
CLUSTER NODES

# 检查节点状态：
# - master - 主节点
# - slave - 从节点
# - fail - 故障节点
# - pfail - 疑似故障
```

### 3. 配置持久化

集群配置会自动保存到文件：

- **JSON 格式**：`cluster_config.json`
- **Redis 格式**：`nodes.conf`

重启节点时会自动加载配置。

### 4. 日志查看

查看服务器日志了解集群状态：

```bash
# 查看启动日志
tail -f server.log

# 关键日志：
# - 节点连接成功
# - 槽分配完成
# - 故障转移事件
```

---

## 最佳实践

### 1. 节点规划

- **最少 3 个主节点**：确保集群可用性
- **主从比例 1:1**：每个主节点至少一个从节点
- **奇数个主节点**：避免投票平局

### 2. 槽分配策略

- **均匀分配**：尽量让每个节点分配的槽数量相近
- **考虑负载**：根据节点性能调整槽分配
- **预留容量**：为未来扩展预留槽空间

### 3. 网络配置

- **专用网络**：集群节点使用专用网络通信
- **防火墙规则**：开放集群端口（默认 7000+）
- **带宽充足**：确保节点间通信带宽充足

### 4. 数据安全

- **启用 AOF**：确保数据持久化
- **定期备份**：备份集群配置和数据
- **监控告警**：设置故障告警机制

### 5. 性能优化

- **使用哈希标签**：将相关键路由到同一节点
- **避免跨槽操作**：减少跨节点操作
- **合理分片**：根据数据特点调整分片策略

### 6. 键命名规范

使用哈希标签确保相关键在同一节点：

```bash
# 好的做法：使用哈希标签
SET {user:1000}:profile "data"
SET {user:1000}:settings "data"
GET {user:1000}:profile  # 两个键在同一节点

# 避免：不使用标签
SET user:1000:profile "data"
SET user:1000:settings "data"  # 可能在不同节点
```

---

## 常见问题

### Q1: 如何添加新节点到集群？

**A:** 
1. 启动新节点
2. 使用 `CLUSTER MEET` 让新节点加入集群
3. 迁移部分槽到新节点
4. 配置从节点（可选）

### Q2: 如何从集群中移除节点？

**A:**
1. 如果是主节点，先迁移所有槽到其他节点
2. 如果是从节点，直接移除
3. 在所有节点上执行 `CLUSTER FORGET <node-id>`

### Q3: 集群支持哪些操作？

**A:**
- ✅ 单键操作：GET, SET, DEL 等
- ✅ 单节点多键操作：同一槽的多个键
- ❌ 跨槽操作：MGET, MSET（需要使用哈希标签）
- ❌ 跨槽事务：MULTI/EXEC（需要使用哈希标签）

### Q4: 如何处理网络分区？

**A:**
- 集群会自动检测网络分区
- 少数派节点会停止服务
- 多数派节点继续服务
- 网络恢复后自动合并

### Q5: 槽迁移会影响服务吗？

**A:**
- 迁移过程中，客户端会收到 ASK 重定向
- 客户端会自动重试到新节点
- 对用户透明，但可能有轻微延迟

### Q6: 如何备份集群数据？

**A:**
1. 备份每个节点的 AOF 文件
2. 备份集群配置文件（`cluster_config.json`）
3. 定期执行 RDB 快照（如果启用）

### Q7: 集群支持多少个节点？

**A:**
- 理论上支持最多 1000 个节点
- 建议 3-50 个节点
- 节点过多会增加通信开销

### Q8: 如何监控集群性能？

**A:**
- 使用 `CLUSTER INFO` 查看集群状态
- 使用 `CLUSTER NODES` 查看节点状态
- 监控每个节点的 QPS、延迟、内存使用

---

## 示例场景

### 场景 1: 3 节点集群部署

```bash
# 节点1
./lingcache-server -addr :7000

# 节点2
./lingcache-server -addr :7001

# 节点3
./lingcache-server -addr :7002

# 在节点1上执行
CLUSTER MEET 127.0.0.1 7001
CLUSTER MEET 127.0.0.1 7002
```

### 场景 2: 添加从节点

```bash
# 启动从节点
./lingcache-server -addr :7003

# 连接到从节点
CLUSTER MEET 127.0.0.1 7000

# 将节点设置为 node1 的从节点
CLUSTER REPLICATE node1
```

### 场景 3: 扩容集群

```bash
# 1. 启动新节点
./lingcache-server -addr :7004

# 2. 加入集群
CLUSTER MEET 127.0.0.1 7004

# 3. 迁移部分槽到新节点
# ... 执行槽迁移 ...
```

---

## 相关文档

- [集群实现总结](./IMPLEMENTATION_SUMMARY.md) - 详细的功能说明
- [集群 README](./README.md) - 功能概述
- [主从复制文档](../replication/) - 主从复制使用指南

---

## 技术支持

如有问题，请查看：
- 代码注释中的详细说明
- 各模块的 README 文件
- 测试文件中的使用示例

---

**最后更新**: 2024-12-20
**版本**: 1.0.0

