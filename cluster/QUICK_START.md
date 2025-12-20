# LingCache 集群快速开始

## 5 分钟快速启动集群

### 1. 编译服务器

```bash
cd /path/to/LingCache
go build -o lingcache-server ./cmd/server
```

### 2. 使用启动脚本（推荐）

```bash
cd cluster/scripts
./start_cluster.sh
```

脚本会自动：
- 创建 3 个节点目录
- 配置每个节点的环境变量
- 启动所有节点

### 3. 节点握手

使用客户端连接到任意节点：

```bash
node ../../client.js
```

执行节点握手：

```bash
CLUSTER MEET 127.0.0.1 7001
CLUSTER MEET 127.0.0.1 7002
```

### 4. 验证集群

```bash
# 查看集群状态
CLUSTER INFO

# 查看节点信息
CLUSTER NODES

# 查看槽分配
CLUSTER SLOTS
```

### 5. 测试数据

```bash
# 设置数据
SET key1 value1
SET key2 value2

# 获取数据
GET key1
GET key2
```

### 6. 停止集群

```bash
cd cluster/scripts
./stop_cluster.sh
```

---

## 手动启动（了解原理）

### 节点 1

```bash
mkdir -p cluster/node1 && cd cluster/node1

cat > .env << EOF
REDIS_ADDR=:7000
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7000
REDIS_CLUSTER_NODE_ID=node1
EOF

../../lingcache-server -addr :7000
```

### 节点 2

```bash
mkdir -p cluster/node2 && cd cluster/node2

cat > .env << EOF
REDIS_ADDR=:7001
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7001
REDIS_CLUSTER_NODE_ID=node2
EOF

../../lingcache-server -addr :7001
```

### 节点 3

```bash
mkdir -p cluster/node3 && cd cluster/node3

cat > .env << EOF
REDIS_ADDR=:7002
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=7002
REDIS_CLUSTER_NODE_ID=node3
EOF

../../lingcache-server -addr :7002
```

---

## 下一步

- 📖 阅读 [完整使用指南](./USAGE_GUIDE.md)
- 🔧 查看 [实现总结](./IMPLEMENTATION_SUMMARY.md)
- 💡 了解 [集群原理](./README.md)

---

**提示**: 生产环境请参考 [USAGE_GUIDE.md](./USAGE_GUIDE.md) 中的最佳实践。

