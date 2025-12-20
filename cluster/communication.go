package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 集群节点通信实现 - Gossip Protocol
 * ============================================================================
 *
 * 【核心原理】
 * Redis Cluster 使用 Gossip 协议进行节点间通信，这是一种去中心化的通信方式。
 *
 * 1. Gossip 协议特点
 *    - 去中心化：没有中心节点，所有节点地位平等
 *    - 随机传播：每个节点随机选择其他节点交换信息
 *    - 最终一致性：信息最终会传播到所有节点
 *    - 容错性强：部分节点故障不影响整体通信
 *
 * 2. 消息类型
 *    - MEET: 节点握手，用于将新节点加入集群
 *    - PING: 心跳检测，定期发送检测节点是否存活
 *    - PONG: 心跳响应，回复 PING 消息
 *    - FAIL: 故障通知，通知其他节点某个节点故障
 *    - PUBLISH: 发布订阅消息
 *    - FAILOVER_AUTH_REQUEST: 故障转移投票请求
 *    - FAILOVER_AUTH_ACK: 故障转移投票确认
 *
 * 3. 通信流程
 *    - 节点启动：监听集群端口（默认 6379 + 10000 = 16379）
 *    - 节点握手：通过 CLUSTER MEET 命令加入集群
 *    - 心跳检测：每 1 秒向随机节点发送 PING
 *    - 信息交换：在 PING/PONG 中携带集群拓扑信息
 *
 * 【面试题】
 * Q1: 为什么 Redis Cluster 使用 Gossip 协议而不是集中式通信？
 * A1: Gossip 协议的优势：
 *     - 去中心化：没有单点故障，更可靠
 *     - 扩展性好：节点增加时通信开销不会线性增长
 *     - 容错性强：部分节点故障不影响整体
 *     集中式通信的问题：
 *     - 中心节点成为瓶颈和单点故障
 *     - 扩展性差：所有节点都要与中心节点通信
 *
 * Q2: Gossip 协议如何保证信息最终传播到所有节点？
 * A2: 通过随机选择和多次传播：
 *     - 每个节点定期随机选择其他节点交换信息
 *     - 信息包含已知的所有节点信息
 *     - 经过多次传播，信息最终会到达所有节点
 *     数学上可以证明，在完全图中，O(log n) 轮传播后信息会到达所有节点
 *
 * Q3: Redis Cluster 的心跳检测机制是什么？
 * A3: 心跳检测流程：
 *     1. 每个节点每 1 秒向随机节点发送 PING
 *     2. 接收节点回复 PONG
 *     3. 如果节点在 N 秒内（默认 15 秒）没有响应，标记为疑似故障
 *     4. 如果超过半数主节点认为节点故障，触发故障转移
 *     这样可以快速检测节点故障，同时避免误判
 *
 * Q4: Redis Cluster 如何处理网络分区？
 * A4: 通过多数派原则：
 *     - 当网络分区时，只有拥有大多数主节点的分区可以继续服务
 *     - 少数派分区会停止接受写请求
 *     - 例如：6 个主节点，分区为 4-2，只有 4 个节点的分区可以服务
 *     这样可以防止数据不一致，但可能降低可用性
 */

// ClusterMessage 集群消息
type ClusterMessage struct {
	Type      string      `json:"type"`      // MEET, PING, PONG, FAILOVER, etc.
	From      string      `json:"from"`      // 发送节点 ID
	To        string      `json:"to"`        // 接收节点 ID
	Data      interface{} `json:"data"`      // 消息数据
	Timestamp int64       `json:"timestamp"` // 时间戳
}

// NodeCommunicator 节点通信器
type NodeCommunicator struct {
	cluster     *Cluster
	connections map[string]net.Conn // nodeID -> connection
	listener    net.Listener
	mu          sync.RWMutex
	running     bool
}

// NewNodeCommunicator 创建节点通信器
func NewNodeCommunicator(cluster *Cluster) *NodeCommunicator {
	return &NodeCommunicator{
		cluster:     cluster,
		connections: make(map[string]net.Conn),
		running:     false,
	}
}

// Start 启动通信服务
func (nc *NodeCommunicator) Start(port int) error {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	nc.listener = listener
	nc.running = true

	// 启动接受连接
	go nc.acceptConnections()

	return nil
}

// acceptConnections 接受连接
func (nc *NodeCommunicator) acceptConnections() {
	for nc.running {
		conn, err := nc.listener.Accept()
		if err != nil {
			continue
		}

		go nc.handleConnection(conn)
	}
}

// handleConnection 处理连接
func (nc *NodeCommunicator) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		// 读取消息
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		var msg ClusterMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}

		// 处理消息
		nc.handleMessage(&msg, conn)
	}
}

// handleMessage 处理消息
func (nc *NodeCommunicator) handleMessage(msg *ClusterMessage, conn net.Conn) {
	switch msg.Type {
	case "MEET":
		nc.handleMeet(msg)
	case "PING":
		nc.handlePing(msg, conn)
	case "PONG":
		nc.handlePong(msg)
	case "FAILOVER":
		nc.handleFailover(msg)
	case "SLOTS":
		nc.handleSlots(msg)
	default:
		// 未知消息类型
	}
}

// handleMeet 处理 MEET 消息
func (nc *NodeCommunicator) handleMeet(msg *ClusterMessage) {
	// 添加新节点到集群
	data, ok := msg.Data.(map[string]interface{})
	if !ok {
		return
	}

	nodeID, _ := data["nodeID"].(string)
	addr, _ := data["addr"].(string)

	nc.cluster.AddNode(nodeID, addr)

	// 发送 PONG 响应
	response := ClusterMessage{
		Type:      "PONG",
		From:      nc.cluster.GetMyself().NodeID,
		To:        msg.From,
		Timestamp: time.Now().Unix(),
	}
	nc.sendMessage(msg.From, &response)
}

// handlePing 处理 PING 消息
func (nc *NodeCommunicator) handlePing(msg *ClusterMessage, conn net.Conn) {
	// 发送 PONG 响应
	response := ClusterMessage{
		Type:      "PONG",
		From:      nc.cluster.GetMyself().NodeID,
		To:        msg.From,
		Timestamp: time.Now().Unix(),
	}
	nc.sendMessageToConn(conn, &response)
}

// handlePong 处理 PONG 消息
func (nc *NodeCommunicator) handlePong(msg *ClusterMessage) {
	// 更新节点状态
	// 实际应该更新心跳时间
}

// handleFailover 处理故障转移消息
func (nc *NodeCommunicator) handleFailover(msg *ClusterMessage) {
	data, ok := msg.Data.(map[string]interface{})
	if !ok {
		return
	}

	_, _ = data["oldMasterID"].(string) // 保留用于日志
	newMasterID, _ := data["newMasterID"].(string)
	slots, _ := data["slots"].([]interface{})

	// 更新槽分配
	slotList := make([]int, 0)
	for _, s := range slots {
		if slot, ok := s.(float64); ok {
			slotList = append(slotList, int(slot))
		}
	}

	nc.cluster.AssignSlots(newMasterID, slotList)
}

// handleSlots 处理槽信息消息
func (nc *NodeCommunicator) handleSlots(msg *ClusterMessage) {
	data, ok := msg.Data.(map[string]interface{})
	if !ok {
		return
	}

	slots, _ := data["slots"].([]interface{})
	nodeID, _ := data["nodeID"].(string)

	slotList := make([]int, 0)
	for _, s := range slots {
		if slot, ok := s.(float64); ok {
			slotList = append(slotList, int(slot))
		}
	}

	nc.cluster.AssignSlots(nodeID, slotList)
}

// SendMeet 发送 MEET 消息
func (nc *NodeCommunicator) SendMeet(targetAddr string, targetNodeID string) error {
	conn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := ClusterMessage{
		Type: "MEET",
		From: nc.cluster.GetMyself().NodeID,
		To:   targetNodeID,
		Data: map[string]interface{}{
			"nodeID": nc.cluster.GetMyself().NodeID,
			"addr":   nc.cluster.GetMyself().Addr,
		},
		Timestamp: time.Now().Unix(),
	}

	return nc.sendMessageToConn(conn, &msg)
}

// sendMessage 发送消息到节点
func (nc *NodeCommunicator) sendMessage(nodeID string, msg *ClusterMessage) error {
	nc.mu.RLock()
	conn, exists := nc.connections[nodeID]
	nc.mu.RUnlock()

	if !exists {
		// 需要先建立连接
		node := nc.cluster.nodes[nodeID]
		if node == nil {
			return fmt.Errorf("node not found: %s", nodeID)
		}

		var err error
		conn, err = net.Dial("tcp", node.Addr)
		if err != nil {
			return err
		}

		nc.mu.Lock()
		nc.connections[nodeID] = conn
		nc.mu.Unlock()
	}

	return nc.sendMessageToConn(conn, msg)
}

// sendMessageToConn 发送消息到连接
func (nc *NodeCommunicator) sendMessageToConn(conn net.Conn, msg *ClusterMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(conn, "%s\n", string(data))
	return err
}

// StartHeartbeat 启动心跳
func (nc *NodeCommunicator) StartHeartbeat() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !nc.running {
			return
		}

		// 向所有节点发送 PING
		nodes := nc.cluster.GetNodes()
		for _, node := range nodes {
			if node.NodeID != nc.cluster.GetMyself().NodeID {
				msg := ClusterMessage{
					Type:      "PING",
					From:      nc.cluster.GetMyself().NodeID,
					To:        node.NodeID,
					Timestamp: time.Now().Unix(),
				}
				nc.sendMessage(node.NodeID, &msg)
			}
		}
	}
}
