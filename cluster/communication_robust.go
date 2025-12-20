package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 集群网络通信健壮性增强 - Robust Communication
 * ============================================================================
 *
 * 【核心原理】
 * 增强网络通信的健壮性，包括连接管理、重连机制、超时处理、错误恢复等。
 *
 * 1. 连接管理
 *    - 连接池：维护到各节点的连接池
 *    - 连接复用：复用连接，减少连接开销
 *    - 连接健康检查：定期检查连接是否健康
 *
 * 2. 重连机制
 *    - 自动重连：连接断开时自动重连
 *    - 指数退避：重连间隔逐渐增加
 *    - 最大重试：限制重连次数
 *
 * 3. 超时处理
 *    - 连接超时：连接建立超时
 *    - 读写超时：读写操作超时
 *    - 请求超时：整个请求超时
 *
 * 4. 错误处理
 *    - 错误分类：区分临时错误和永久错误
 *    - 错误重试：临时错误自动重试
 *    - 错误恢复：永久错误需要人工干预
 *
 * 【面试题】
 * Q1: 如何实现可靠的节点间通信？
 * A1: 可靠性保证：
 *     1. **连接管理**：使用连接池，复用连接
 *     2. **心跳检测**：定期发送心跳，检测连接健康
 *     3. **自动重连**：连接断开时自动重连
 *     4. **超时处理**：设置合理的超时时间
 *     5. **错误重试**：临时错误自动重试
 *     6. **消息确认**：重要消息需要确认
 *     实现：
 *     - 维护连接状态
 *     - 实现重连逻辑
 *     - 设置超时和重试
 *
 * Q2: 网络分区时如何保证通信？
 * A2: 分区处理：
 *     1. **检测分区**：通过心跳检测发现分区
 *     2. **多数派原则**：只有多数派可以通信
 *     3. **消息缓冲**：分区期间缓冲消息
 *     4. **分区恢复**：分区恢复后同步消息
 *     5. **冲突解决**：处理分区期间的冲突
 *     例如：
 *     - 6 个节点，分区为 4-2
 *     - 4 个节点的分区可以继续通信
 *     - 2 个节点的分区停止服务
 *     - 分区恢复后，同步状态
 *
 * Q3: 如何优化集群通信性能？
 * A3: 性能优化：
 *     1. **连接复用**：使用连接池，减少连接开销
 *     2. **批量操作**：批量发送消息，减少网络往返
 *     3. **压缩传输**：对大数据进行压缩
 *     4. **异步通信**：使用异步通信，不阻塞主流程
 *     5. **本地缓存**：缓存集群拓扑，减少查询
 *     6. **并行通信**：并行发送到多个节点
 *     优化效果：
 *     - 减少延迟：连接复用减少连接建立时间
 *     - 提高吞吐：批量操作提高吞吐量
 *     - 节省带宽：压缩传输节省带宽
 *
 * Q4: 如何处理网络抖动？
 * A4: 抖动处理：
 *     1. **重试机制**：临时失败自动重试
 *     2. **超时设置**：设置合理的超时时间
 *     3. **指数退避**：重试间隔逐渐增加
 *     4. **快速失败**：永久错误快速失败
 *     5. **降级策略**：网络问题时降级服务
 *     实现：
 *     - 区分临时错误和永久错误
 *     - 临时错误重试，永久错误快速失败
 *     - 使用指数退避避免网络拥塞
 *
 * Q5: 如何实现连接的健康检查？
 * A5: 健康检查机制：
 *     1. **定期心跳**：定期发送 PING，检查连接
 *     2. **超时检测**：如果超时未收到响应，标记为不健康
 *     3. **自动恢复**：不健康的连接自动重建
 *     4. **状态监控**：监控连接状态变化
 *     实现：
 *     - 每个连接维护最后心跳时间
 *     - 定期检查心跳超时
 *     - 超时连接自动重建
 */

// ConnectionState 连接状态
type ConnectionState int

const (
	CONN_STATE_DISCONNECTED ConnectionState = iota
	CONN_STATE_CONNECTING
	CONN_STATE_CONNECTED
	CONN_STATE_RECONNECTING
)

// RobustConnection 健壮的连接
type RobustConnection struct {
	nodeID               string
	addr                 string
	conn                 net.Conn
	state                ConnectionState
	mu                   sync.RWMutex
	lastPing             time.Time
	lastPong             time.Time
	reconnectAttempts    int
	maxReconnectAttempts int
	reconnectInterval    time.Duration
	readTimeout          time.Duration
	writeTimeout         time.Duration
	ctx                  context.Context
	cancel               context.CancelFunc
}

// NewRobustConnection 创建健壮的连接
func NewRobustConnection(nodeID, addr string) *RobustConnection {
	ctx, cancel := context.WithCancel(context.Background())

	return &RobustConnection{
		nodeID:               nodeID,
		addr:                 addr,
		state:                CONN_STATE_DISCONNECTED,
		maxReconnectAttempts: 10,
		reconnectInterval:    1 * time.Second,
		readTimeout:          5 * time.Second,
		writeTimeout:         5 * time.Second,
		ctx:                  ctx,
		cancel:               cancel,
	}
}

// Connect 连接
func (rc *RobustConnection) Connect() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.state == CONN_STATE_CONNECTED {
		return nil
	}

	rc.state = CONN_STATE_CONNECTING

	conn, err := net.DialTimeout("tcp", rc.addr, 5*time.Second)
	if err != nil {
		rc.state = CONN_STATE_DISCONNECTED
		return err
	}

	rc.conn = conn
	rc.state = CONN_STATE_CONNECTED
	rc.reconnectAttempts = 0
	rc.lastPong = time.Now()

	// 启动健康检查
	go rc.healthCheck()

	return nil
}

// Reconnect 重连
func (rc *RobustConnection) Reconnect() error {
	rc.mu.Lock()

	if rc.reconnectAttempts >= rc.maxReconnectAttempts {
		rc.mu.Unlock()
		return errors.New("max reconnect attempts reached")
	}

	rc.state = CONN_STATE_RECONNECTING
	rc.reconnectAttempts++
	interval := rc.reconnectInterval * time.Duration(rc.reconnectAttempts) // 指数退避
	rc.mu.Unlock()

	// 等待后重连
	time.Sleep(interval)

	return rc.Connect()
}

// Write 写入数据（带超时）
func (rc *RobustConnection) Write(data []byte) error {
	rc.mu.RLock()
	conn := rc.conn
	state := rc.state
	rc.mu.RUnlock()

	if state != CONN_STATE_CONNECTED || conn == nil {
		return errors.New("connection not established")
	}

	// 设置写超时
	if err := conn.SetWriteDeadline(time.Now().Add(rc.writeTimeout)); err != nil {
		return err
	}

	_, err := conn.Write(data)
	if err != nil {
		// 连接错误，触发重连
		go rc.Reconnect()
		return err
	}

	return nil
}

// Read 读取数据（带超时）
func (rc *RobustConnection) Read(buffer []byte) (int, error) {
	rc.mu.RLock()
	conn := rc.conn
	state := rc.state
	rc.mu.RUnlock()

	if state != CONN_STATE_CONNECTED || conn == nil {
		return 0, errors.New("connection not established")
	}

	// 设置读超时
	if err := conn.SetReadDeadline(time.Now().Add(rc.readTimeout)); err != nil {
		return 0, err
	}

	n, err := conn.Read(buffer)
	if err != nil {
		// 连接错误，触发重连
		go rc.Reconnect()
		return 0, err
	}

	return n, nil
}

// healthCheck 健康检查
func (rc *RobustConnection) healthCheck() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			rc.mu.RLock()
			state := rc.state
			lastPong := rc.lastPong
			rc.mu.RUnlock()

			if state != CONN_STATE_CONNECTED {
				continue
			}

			// 检查心跳超时（30 秒）
			if time.Since(lastPong) > 30*time.Second {
				// 连接不健康，触发重连
				go rc.Reconnect()
			}
		}
	}
}

// UpdateLastPong 更新最后 PONG 时间
func (rc *RobustConnection) UpdateLastPong() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.lastPong = time.Now()
}

// Close 关闭连接
func (rc *RobustConnection) Close() error {
	rc.cancel()

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.conn != nil {
		err := rc.conn.Close()
		rc.conn = nil
		rc.state = CONN_STATE_DISCONNECTED
		return err
	}

	return nil
}

// GetState 获取连接状态
func (rc *RobustConnection) GetState() ConnectionState {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.state
}

// ConnectionPool 连接池
type ConnectionPool struct {
	connections map[string]*RobustConnection // nodeID -> connection
	mu          sync.RWMutex
}

// NewConnectionPool 创建连接池
func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*RobustConnection),
	}
}

// GetConnection 获取连接
func (cp *ConnectionPool) GetConnection(nodeID, addr string) (*RobustConnection, error) {
	cp.mu.RLock()
	conn, exists := cp.connections[nodeID]
	cp.mu.RUnlock()

	if exists && conn.GetState() == CONN_STATE_CONNECTED {
		return conn, nil
	}

	// 创建新连接
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// 再次检查（双重检查锁定）
	if conn, exists := cp.connections[nodeID]; exists && conn.GetState() == CONN_STATE_CONNECTED {
		return conn, nil
	}

	// 创建新连接
	conn = NewRobustConnection(nodeID, addr)
	if err := conn.Connect(); err != nil {
		return nil, err
	}

	cp.connections[nodeID] = conn
	return conn, nil
}

// RemoveConnection 移除连接
func (cp *ConnectionPool) RemoveConnection(nodeID string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if conn, exists := cp.connections[nodeID]; exists {
		conn.Close()
		delete(cp.connections, nodeID)
	}
}

// CloseAll 关闭所有连接
func (cp *ConnectionPool) CloseAll() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, conn := range cp.connections {
		conn.Close()
	}

	cp.connections = make(map[string]*RobustConnection)
}

// RobustNodeCommunicator 健壮的节点通信器
type RobustNodeCommunicator struct {
	cluster *Cluster
	pool    *ConnectionPool
	mu      sync.RWMutex
}

// NewRobustNodeCommunicator 创建健壮的节点通信器
func NewRobustNodeCommunicator(cluster *Cluster) *RobustNodeCommunicator {
	return &RobustNodeCommunicator{
		cluster: cluster,
		pool:    NewConnectionPool(),
	}
}

// SendMessageRobust 健壮地发送消息
func (rnc *RobustNodeCommunicator) SendMessageRobust(nodeID string, msg *ClusterMessage) error {
	// 获取节点信息
	rnc.cluster.mu.RLock()
	node, exists := rnc.cluster.nodes[nodeID]
	rnc.cluster.mu.RUnlock()

	if !exists {
		return errors.New("node not found")
	}

	// 获取连接
	conn, err := rnc.pool.GetConnection(nodeID, node.Addr)
	if err != nil {
		return err
	}

	// 序列化消息
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// 发送消息（带重试）
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		if err := conn.Write(append(data, '\n')); err == nil {
			return nil
		}

		// 重试前等待
		if i < maxRetries-1 {
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}

	return errors.New("failed to send message after retries")
}

// ReadMessageRobust 健壮地读取消息
func (rnc *RobustNodeCommunicator) ReadMessageRobust(nodeID string) (*ClusterMessage, error) {
	// 获取节点信息
	rnc.cluster.mu.RLock()
	node, exists := rnc.cluster.nodes[nodeID]
	rnc.cluster.mu.RUnlock()

	if !exists {
		return nil, errors.New("node not found")
	}

	// 获取连接
	conn, err := rnc.pool.GetConnection(nodeID, node.Addr)
	if err != nil {
		return nil, err
	}

	// 读取消息
	conn.mu.RLock()
	netConn := conn.conn
	conn.mu.RUnlock()

	if netConn == nil {
		return nil, errors.New("connection not established")
	}

	reader := bufio.NewReader(netConn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	// 反序列化
	var msg ClusterMessage
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}
