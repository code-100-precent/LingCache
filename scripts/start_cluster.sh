#!/bin/bash

# LingCache 集群启动脚本
# 用于快速启动一个 3 节点集群（测试环境）

set -e

# 配置
BASE_PORT=7000
NODE_COUNT=3
CLUSTER_DIR="./cluster_nodes"

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== LingCache 集群启动脚本 ===${NC}\n"

# 检查服务器可执行文件
if [ ! -f "./lingcache-server" ]; then
    echo -e "${YELLOW}警告: 未找到 lingcache-server，尝试编译...${NC}"
    cd ..
    go build -o lingcache-server ./cmd/server
    cd cluster/scripts
fi

# 创建集群目录
mkdir -p $CLUSTER_DIR
cd $CLUSTER_DIR

# 启动节点
echo -e "${GREEN}启动集群节点...${NC}"

for i in $(seq 1 $NODE_COUNT); do
    PORT=$((BASE_PORT + i - 1))
    NODE_DIR="node$i"
    
    # 创建节点目录
    mkdir -p $NODE_DIR
    cd $NODE_DIR
    
    # 创建配置文件
    cat > .env << EOF
REDIS_ADDR=:$PORT
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_PORT=$PORT
REDIS_CLUSTER_NODE_ID=node$i
REDIS_AOF_ENABLED=true
REDIS_AOF_FILENAME=appendonly.aof
REDIS_RDB_ENABLED=true
REDIS_RDB_FILENAME=dump.rdb
EOF
    
    # 启动节点（后台运行）
    echo -e "${GREEN}启动节点 $i (端口: $PORT)...${NC}"
    ../../lingcache-server -addr :$PORT > server.log 2>&1 &
    echo $! > server.pid
    
    cd ..
    sleep 1
done

echo -e "\n${GREEN}所有节点已启动！${NC}\n"

# 等待节点启动
echo -e "${YELLOW}等待节点启动...${NC}"
sleep 3

# 节点握手
echo -e "${GREEN}执行节点握手...${NC}"
echo -e "${YELLOW}注意: 需要手动执行以下命令让节点互相认识:${NC}"
echo ""
for i in $(seq 2 $NODE_COUNT); do
    PORT=$((BASE_PORT + i - 1))
    echo "  CLUSTER MEET 127.0.0.1 $PORT"
done
echo ""
echo -e "${YELLOW}可以使用以下命令连接并执行:${NC}"
echo "  node ../../client.js"
echo ""

# 显示节点信息
echo -e "${GREEN}节点信息:${NC}"
for i in $(seq 1 $NODE_COUNT); do
    PORT=$((BASE_PORT + i - 1))
    NODE_DIR="node$i"
    PID=$(cat $NODE_DIR/server.pid 2>/dev/null || echo "N/A")
    echo "  节点 $i: 端口 $PORT, PID: $PID"
done

echo ""
echo -e "${GREEN}集群启动完成！${NC}"
echo -e "${YELLOW}停止集群: ./stop_cluster.sh${NC}"
echo -e "${YELLOW}查看日志: tail -f cluster_nodes/node*/server.log${NC}"

