package cluster

import (
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 集群故障转移实现 - Automatic Failover
 * ============================================================================
 *
 * 【核心原理】
 * Redis Cluster 的故障转移是自动的，当主节点故障时，从节点会自动提升为主节点。
 *
 * 1. 故障检测
 *    - 心跳超时：主节点在指定时间内（默认 15 秒）没有响应 PONG
 *    - 投票机制：其他主节点投票决定是否认为该节点故障
 *    - 故障标记：超过半数主节点认为故障，标记为 FAIL
 *
 * 2. 从节点选举
 *    - 资格检查：从节点必须与主节点断开连接超过一定时间（默认 10 秒）
 *    - 延迟选举：从节点等待随机延迟（0-500ms），避免同时选举
 *    - 投票请求：从节点向其他主节点发送 FAILOVER_AUTH_REQUEST
 *    - 投票确认：主节点回复 FAILOVER_AUTH_ACK（每个主节点只能投一票）
 *    - 选举成功：获得超过半数主节点投票的从节点成为新主节点
 *
 * 3. 槽转移
 *    - 新主节点接管原主节点的所有槽
 *    - 更新集群拓扑，通知所有节点
 *    - 客户端收到 MOVED 重定向，更新路由缓存
 *
 * 4. 数据一致性
 *    - 异步复制：主从复制是异步的，可能丢失部分数据
 *    - 复制偏移量：通过复制偏移量判断数据同步情况
 *    - 最佳从节点：选择复制偏移量最大的从节点作为新主节点
 *
 * 【面试题】
 * Q1: Redis Cluster 的故障转移为什么需要投票机制？
 * A1: 投票机制的作用：
 *     - 防止误判：单个节点故障检测可能误判，需要多数节点确认
 *     - 防止脑裂：网络分区时，只有多数派可以选举新主节点
 *     - 保证一致性：确保只有一个从节点被选举为新主节点
 *     例如：6 个主节点，需要至少 4 个主节点投票才能确认故障
 *
 * Q2: 从节点选举的延迟机制是什么？
 * A2: 延迟选举机制：
 *     - 随机延迟：从节点等待 0-500ms 的随机延迟
 *     - 目的：避免多个从节点同时发起选举，造成冲突
 *     - 优先级：复制偏移量大的从节点延迟更短，更容易被选举
 *     这样可以提高选举效率，减少冲突
 *
 * Q3: Redis Cluster 的故障转移会丢失数据吗？
 * A3: 可能丢失数据：
 *     - 异步复制：主从复制是异步的，主节点写入后可能还没复制到从节点
 *     - 故障时机：如果主节点在数据复制前故障，数据会丢失
 *     - 最小化丢失：选择复制偏移量最大的从节点，减少数据丢失
 *     如果需要强一致性，可以使用 WAIT 命令等待复制完成
 *
 * Q4: Redis Cluster 的故障转移时间是多少？
 * A4: 故障转移时间：
 *     - 故障检测：默认 15 秒（NODE_TIMEOUT）
 *     - 选举延迟：0-500ms 随机延迟
 *     - 投票时间：通常几毫秒到几十毫秒
 *     - 槽转移：几乎瞬时（只是更新映射）
 *     总时间：通常在 15-20 秒内完成
 *     可以通过调整 NODE_TIMEOUT 参数优化，但太小可能误判
 *
 * Q5: Redis Cluster 如何选择最佳的从节点作为新主节点？
 * A5: 选择标准：
 *     - 复制偏移量：选择复制偏移量最大的从节点（数据最新）
 *     - 连接时间：从节点必须与主节点断开连接超过一定时间
 *     - 节点优先级：可以设置节点优先级，优先级高的优先选举
 *     这样可以最大化数据保留，减少数据丢失
 *
 * Q6: Redis Cluster 的故障转移和 Redis Sentinel 有什么区别？
 * A6: 主要区别：
 *     - **触发方式**：
 *       * Cluster：自动触发，从节点自动选举
 *       * Sentinel：Sentinel 节点监控并触发故障转移
 *     - **选举机制**：
 *       * Cluster：从节点投票选举
 *       * Sentinel：Sentinel 节点投票选举
 *     - **复杂度**：
 *       * Cluster：更复杂，需要处理槽分配
 *       * Sentinel：相对简单，只是主从切换
 */

// FailoverManager 故障转移管理器
type FailoverManager struct {
	cluster           *Cluster
	nodes             map[string]*NodeStatus // nodeID -> status
	mu                sync.RWMutex
	heartbeatInterval time.Duration
	failoverTimeout   time.Duration
}

// NodeStatus 节点状态
type NodeStatus struct {
	NodeID   string
	LastSeen time.Time
	IsAlive  bool
	IsMaster bool
	Replicas []string
	Slots    []int
}

// NewFailoverManager 创建故障转移管理器
func NewFailoverManager(cluster *Cluster) *FailoverManager {
	return &FailoverManager{
		cluster:           cluster,
		nodes:             make(map[string]*NodeStatus),
		heartbeatInterval: 1 * time.Second,
		failoverTimeout:   5 * time.Second,
	}
}

// Start 启动故障转移管理器
func (fm *FailoverManager) Start() {
	// 启动心跳检测
	go fm.heartbeatLoop()

	// 启动故障检测
	go fm.failoverLoop()
}

// heartbeatLoop 心跳检测循环
func (fm *FailoverManager) heartbeatLoop() {
	ticker := time.NewTicker(fm.heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		fm.checkHeartbeat()
	}
}

// checkHeartbeat 检查心跳
func (fm *FailoverManager) checkHeartbeat() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	now := time.Now()
	for nodeID, status := range fm.nodes {
		if status.IsMaster {
			// 检查主节点是否超时
			if now.Sub(status.LastSeen) > fm.failoverTimeout {
				status.IsAlive = false
				// 触发故障转移
				go fm.triggerFailover(nodeID)
			}
		}
	}
}

// failoverLoop 故障转移循环
func (fm *FailoverManager) failoverLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fm.checkFailover()
	}
}

// checkFailover 检查是否需要故障转移
func (fm *FailoverManager) checkFailover() {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	for nodeID, status := range fm.nodes {
		if status.IsMaster && !status.IsAlive {
			// 主节点故障，触发故障转移
			fm.triggerFailover(nodeID)
		}
	}
}

// triggerFailover 触发故障转移
func (fm *FailoverManager) triggerFailover(failedNodeID string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	failedNode, exists := fm.nodes[failedNodeID]
	if !exists || !failedNode.IsMaster {
		return
	}

	// 找到该主节点的从节点
	replicas := failedNode.Replicas
	if len(replicas) == 0 {
		// 没有从节点，无法故障转移
		return
	}

	// 选举新的主节点（选择第一个从节点）
	newMasterID := replicas[0]
	if _, exists := fm.nodes[newMasterID]; !exists {
		return
	}

	// 执行故障转移
	fm.performFailover(failedNodeID, newMasterID, failedNode.Slots)
}

// performFailover 执行故障转移
func (fm *FailoverManager) performFailover(oldMasterID, newMasterID string, slots []int) {
	// 更新节点状态
	newMaster := fm.nodes[newMasterID]
	newMaster.IsMaster = true
	newMaster.Slots = slots
	newMaster.IsAlive = true
	newMaster.LastSeen = time.Now()

	// 从旧主节点移除
	delete(fm.nodes, oldMasterID)

	// 更新集群槽分配
	for _, slot := range slots {
		node := fm.cluster.GetSlotNode(slot)
		if node != nil && node.NodeID == oldMasterID {
			// 更新槽到新主节点
			newNode := fm.cluster.nodes[newMasterID]
			if newNode != nil {
				fm.cluster.AssignSlots(newMasterID, []int{slot})
			}
		}
	}

	// 通知其他节点
	fm.notifyNodes(oldMasterID, newMasterID, slots)
}

// notifyNodes 通知其他节点故障转移
func (fm *FailoverManager) notifyNodes(oldMasterID, newMasterID string, slots []int) {
	// 简化实现：更新所有节点的视图
	for nodeID := range fm.nodes {
		if nodeID != newMasterID {
			// 通知节点更新槽分配
			// 实际应该通过网络协议通知
		}
	}
}

// UpdateNodeStatus 更新节点状态
func (fm *FailoverManager) UpdateNodeStatus(nodeID string, isAlive bool) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	status, exists := fm.nodes[nodeID]
	if !exists {
		status = &NodeStatus{
			NodeID:   nodeID,
			IsAlive:  isAlive,
			IsMaster: false,
			Replicas: make([]string, 0),
			Slots:    make([]int, 0),
		}
		fm.nodes[nodeID] = status
	}

	status.LastSeen = time.Now()
	status.IsAlive = isAlive
}

// RegisterNode 注册节点
func (fm *FailoverManager) RegisterNode(nodeID string, isMaster bool, slots []int, replicas []string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	status := &NodeStatus{
		NodeID:   nodeID,
		IsAlive:  true,
		IsMaster: isMaster,
		Replicas: replicas,
		Slots:    slots,
		LastSeen: time.Now(),
	}

	fm.nodes[nodeID] = status
}
