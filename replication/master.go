package replication

import (
	"bufio"
	"fmt"
	"net"
	"sync"

	"github.com/code-100-precent/LingCache/persistence"
	"github.com/code-100-precent/LingCache/protocol"
	"github.com/code-100-precent/LingCache/storage"
)

/*
 * ============================================================================
 * Redis 主从复制实现（主节点）
 * ============================================================================
 *
 * Redis 主从复制流程：
 * 1. 从节点连接主节点，发送 REPLCONF 命令
 * 2. 主节点发送 RDB 文件（全量同步）
 * 3. 主节点持续发送写命令（增量同步）
 *
 * 【复制协议】
 * - REPLCONF: 配置复制
 * - PSYNC: 部分同步请求
 * - FULLRESYNC: 全量同步
 */

// Master 主节点
type Master struct {
	server      *storage.RedisServer
	replicas    map[*Replica]bool // 从节点集合
	mu          sync.RWMutex
	replOffset  int64  // 复制偏移量
	replBacklog []byte // 复制积压缓冲区
}

// Replica 从节点连接
type Replica struct {
	conn   net.Conn
	writer *bufio.Writer
	master *Master
	offset int64
	closed bool
}

// NewMaster 创建主节点
func NewMaster(server *storage.RedisServer) *Master {
	return &Master{
		server:      server,
		replicas:    make(map[*Replica]bool),
		replOffset:  0,
		replBacklog: make([]byte, 0),
	}
}

// AddReplica 添加从节点
func (m *Master) AddReplica(conn net.Conn) *Replica {
	m.mu.Lock()
	defer m.mu.Unlock()

	replica := &Replica{
		conn:   conn,
		writer: bufio.NewWriter(conn),
		master: m,
		offset: 0,
		closed: false,
	}

	m.replicas[replica] = true

	// 启动全量同步
	go m.fullResync(replica)

	return replica
}

// fullResync 全量同步
func (m *Master) fullResync(replica *Replica) {
	// 发送 FULLRESYNC 响应
	replID := "0000000000000000000000000000000000000000"
	offset := m.replOffset

	response := fmt.Sprintf("FULLRESYNC %s %d\r\n", replID, offset)
	replica.writer.WriteString(response)
	replica.writer.Flush()

	// 生成 RDB 文件并发送
	encoder := persistence.NewRDBEncoder(replica.writer)
	err := encoder.Save(m.server, "")
	if err != nil {
		// 如果失败，发送空的 RDB
		rdbHeader := "REDIS0009"
		replica.writer.WriteString(rdbHeader)
		replica.writer.Flush()
	}

	// 增量同步通过 PropagateCommand 方法实现
	// 不需要单独的 incrementalSync goroutine
}

// PropagateCommand 传播命令到所有从节点
func (m *Master) PropagateCommand(cmd *protocol.RESPValue) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 更新复制偏移量
	m.replOffset += int64(len(cmd.Encode()))

	// 发送给所有从节点
	for replica := range m.replicas {
		if !replica.closed {
			data := cmd.Encode()
			replica.writer.Write(data)
			replica.writer.Flush()
		}
	}
}

// RemoveReplica 移除从节点
func (m *Master) RemoveReplica(replica *Replica) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.replicas, replica)
}
