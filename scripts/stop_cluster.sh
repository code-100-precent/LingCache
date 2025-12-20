#!/bin/bash

# LingCache 集群停止脚本

set -e

CLUSTER_DIR="./cluster_nodes"

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== 停止 LingCache 集群 ===${NC}\n"

if [ ! -d "$CLUSTER_DIR" ]; then
    echo -e "${RED}错误: 集群目录不存在${NC}"
    exit 1
fi

cd $CLUSTER_DIR

# 停止所有节点
for node_dir in node*; do
    if [ -d "$node_dir" ]; then
        PID_FILE="$node_dir/server.pid"
        if [ -f "$PID_FILE" ]; then
            PID=$(cat $PID_FILE)
            if ps -p $PID > /dev/null 2>&1; then
                echo -e "${YELLOW}停止节点: $node_dir (PID: $PID)${NC}"
                kill $PID
                sleep 1
            else
                echo -e "${YELLOW}节点 $node_dir 已停止${NC}"
            fi
            rm -f $PID_FILE
        fi
    fi
done

echo -e "\n${GREEN}所有节点已停止${NC}"

