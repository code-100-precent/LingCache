package cluster

import (
	"fmt"
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 集群监控和统计 - Cluster Monitoring & Statistics
 * ============================================================================
 *
 * 【核心原理】
 * 集群监控用于收集和展示集群的运行状态、性能指标和统计信息。
 * 这对于运维、性能优化和故障排查非常重要。
 *
 * 1. 监控指标
 *    - 节点状态：在线/离线、主从角色
 *    - 槽分配：每个节点负责的槽数量和范围
 *    - 性能指标：QPS、延迟、内存使用
 *    - 网络指标：连接数、带宽使用
 *    - 错误统计：错误率、重定向次数
 *
 * 2. 统计信息
 *    - 集群总键数
 *    - 每个节点的键数
 *    - 每个槽的键数
 *    - 内存使用情况
 *    - 命令执行统计
 *
 * 3. 健康检查
 *    - 节点健康状态
 *    - 槽分配完整性
 *    - 主从同步状态
 *    - 网络连通性
 *
 * 【面试题】
 * Q1: Redis Cluster 如何监控集群健康状态？
 * A1: 监控维度：
 *     1. **节点状态**：通过心跳检测节点是否在线
 *     2. **槽分配**：检查所有槽是否都有节点负责
 *     3. **主从同步**：检查从节点是否与主节点同步
 *     4. **性能指标**：QPS、延迟、内存使用
 *     5. **错误率**：命令失败率、重定向率
 *     健康检查命令：
 *     - CLUSTER INFO：集群基本信息
 *      - CLUSTER NODES：节点详细信息
 *     - CLUSTER SLOTS：槽分配信息
 *
 * Q2: 如何监控槽迁移的进度？
 * A2: 监控方式：
 *     1. **迁移状态**：查询槽的迁移状态（MIGRATING/IMPORTING）
 *     2. **键数量**：统计源节点和目标节点的键数量
 *     3. **迁移进度**：已迁移键数 / 总键数
 *     4. **迁移速度**：单位时间内迁移的键数
 *     5. **剩余时间**：根据迁移速度估算剩余时间
 *     实现：
 *     - 定期查询迁移状态
 *     - 统计键数量变化
 *     - 计算迁移进度百分比
 *
 * Q3: 集群性能监控的关键指标有哪些？
 * A3: 关键指标：
 *     1. **QPS（每秒查询数）**：集群总 QPS 和节点 QPS
 *     2. **延迟**：命令执行延迟（P50、P99、P999）
 *     3. **内存使用**：每个节点的内存使用情况
 *     4. **网络流量**：入站和出站流量
 *     5. **连接数**：客户端连接数
 *     6. **错误率**：命令失败率、重定向率
 *     7. **槽分布**：槽的键数量分布（是否均匀）
 *     这些指标可以帮助：
 *     - 识别性能瓶颈
 *     - 优化槽分配
 *     - 预测容量需求
 *
 * Q4: 如何实现集群的实时监控？
 * A4: 实现方式：
 *     1. **指标收集**：定期收集各项指标
 *     2. **数据存储**：使用时间序列数据库（如 Prometheus）
 *     3. **可视化**：使用 Grafana 等工具展示
 *     4. **告警**：设置阈值，超过阈值时告警
 *     5. **日志**：记录关键事件和错误
 *     架构：
 *     - 监控代理：在每个节点运行，收集指标
 *     - 监控中心：聚合所有节点的指标
 *     - 告警系统：根据指标触发告警
 *
 * Q5: 集群监控如何帮助故障排查？
 * A5: 故障排查流程：
 *     1. **查看监控面板**：检查各项指标是否异常
 *     2. **定位问题节点**：找出性能异常的节点
 *     3. **分析指标趋势**：查看指标的历史趋势
 *     4. **检查错误日志**：查看错误日志定位问题
 *     5. **验证修复效果**：修复后观察指标是否恢复正常
 *     常见问题：
 *     - 节点故障：QPS 下降、错误率上升
 *     - 槽迁移：重定向率上升、延迟增加
 *     - 内存不足：内存使用率接近 100%
 *     - 网络问题：连接数异常、延迟增加
 */

// ClusterMetrics 集群指标
type ClusterMetrics struct {
	TotalNodes      int                     `json:"total_nodes"`
	MasterNodes     int                     `json:"master_nodes"`
	SlaveNodes      int                     `json:"slave_nodes"`
	TotalSlots      int                     `json:"total_slots"`
	AssignedSlots   int                     `json:"assigned_slots"`
	UnassignedSlots int                     `json:"unassigned_slots"`
	TotalKeys       int64                   `json:"total_keys"`
	NodeMetrics     map[string]*NodeMetrics `json:"node_metrics"`
	SlotMetrics     map[int]*SlotMetrics    `json:"slot_metrics"`
	LastUpdate      int64                   `json:"last_update"`
}

// NodeMetrics 节点指标
type NodeMetrics struct {
	NodeID        string  `json:"node_id"`
	Addr          string  `json:"addr"`
	Role          string  `json:"role"`           // master/slave
	Status        string  `json:"status"`         // connected/disconnected
	SlotsCount    int     `json:"slots_count"`    // 负责的槽数量
	KeysCount     int64   `json:"keys_count"`     // 键数量
	QPS           float64 `json:"qps"`            // 每秒查询数
	Latency       float64 `json:"latency"`        // 平均延迟（毫秒）
	MemoryUsed    int64   `json:"memory_used"`    // 内存使用（字节）
	Connections   int     `json:"connections"`    // 连接数
	Errors        int64   `json:"errors"`         // 错误数
	Redirects     int64   `json:"redirects"`      // 重定向次数
	LastHeartbeat int64   `json:"last_heartbeat"` // 最后心跳时间
}

// SlotMetrics 槽指标
type SlotMetrics struct {
	Slot      int    `json:"slot"`
	NodeID    string `json:"node_id"`
	KeysCount int64  `json:"keys_count"` // 槽中的键数量
	State     string `json:"state"`      // NORMAL/MIGRATING/IMPORTING
}

// ClusterMonitor 集群监控器
type ClusterMonitor struct {
	cluster        *Cluster
	metrics        *ClusterMetrics
	mu             sync.RWMutex
	updateInterval time.Duration
	running        bool
}

// NewClusterMonitor 创建集群监控器
func NewClusterMonitor(cluster *Cluster) *ClusterMonitor {
	return &ClusterMonitor{
		cluster:        cluster,
		metrics:        &ClusterMetrics{},
		updateInterval: 5 * time.Second,
		running:        false,
	}
}

// Start 启动监控
func (cm *ClusterMonitor) Start() {
	if cm.running {
		return
	}

	cm.running = true
	go cm.monitoringLoop()
}

// Stop 停止监控
func (cm *ClusterMonitor) Stop() {
	cm.running = false
}

// monitoringLoop 监控循环
func (cm *ClusterMonitor) monitoringLoop() {
	ticker := time.NewTicker(cm.updateInterval)
	defer ticker.Stop()

	for range ticker.C {
		if !cm.running {
			return
		}

		cm.UpdateMetrics()
	}
}

// UpdateMetrics 更新指标
func (cm *ClusterMonitor) UpdateMetrics() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	metrics := &ClusterMetrics{
		NodeMetrics: make(map[string]*NodeMetrics),
		SlotMetrics: make(map[int]*SlotMetrics),
		LastUpdate:  time.Now().Unix(),
	}

	// 收集节点指标
	nodes := cm.cluster.GetNodes()
	metrics.TotalNodes = len(nodes)

	for _, node := range nodes {
		nodeMetrics := &NodeMetrics{
			NodeID:     node.NodeID,
			Addr:       node.Addr,
			SlotsCount: len(node.Slots),
			Status:     "connected",
		}

		if node.Master != nil {
			nodeMetrics.Role = "slave"
			metrics.SlaveNodes++
		} else {
			nodeMetrics.Role = "master"
			metrics.MasterNodes++
		}

		// 统计槽中的键数量（简化实现）
		nodeMetrics.KeysCount = cm.countKeysInNode(node)

		metrics.NodeMetrics[node.NodeID] = nodeMetrics
	}

	// 收集槽指标
	slots := cm.cluster.GetSlots()
	metrics.TotalSlots = CLUSTER_SLOTS

	for slot, node := range slots {
		if node != nil {
			metrics.AssignedSlots++
			slotMetrics := &SlotMetrics{
				Slot:      slot,
				NodeID:    node.NodeID,
				KeysCount: 0, // 简化实现
				State:     "NORMAL",
			}

			// 检查是否在迁移
			if cm.cluster.reshardingMgr != nil {
				if cm.cluster.reshardingMgr.IsSlotMigrating(slot) {
					slotMetrics.State = "MIGRATING"
				} else if cm.cluster.reshardingMgr.IsSlotImporting(slot) {
					slotMetrics.State = "IMPORTING"
				}
			}

			metrics.SlotMetrics[slot] = slotMetrics
		} else {
			metrics.UnassignedSlots++
		}
	}

	// 计算总键数
	for _, nodeMetrics := range metrics.NodeMetrics {
		metrics.TotalKeys += nodeMetrics.KeysCount
	}

	cm.metrics = metrics
}

// countKeysInNode 统计节点中的键数量（简化实现）
func (cm *ClusterMonitor) countKeysInNode(node *ClusterNode) int64 {
	// 实际应该从存储层统计
	// 这里简化实现，返回槽数量作为估算
	return int64(len(node.Slots) * 100) // 假设每个槽平均 100 个键
}

// GetMetrics 获取指标
func (cm *ClusterMonitor) GetMetrics() *ClusterMetrics {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.metrics
}

// GetNodeMetrics 获取节点指标
func (cm *ClusterMonitor) GetNodeMetrics(nodeID string) (*NodeMetrics, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	metrics, exists := cm.metrics.NodeMetrics[nodeID]
	return metrics, exists
}

// GetSlotMetrics 获取槽指标
func (cm *ClusterMonitor) GetSlotMetrics(slot int) (*SlotMetrics, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	metrics, exists := cm.metrics.SlotMetrics[slot]
	return metrics, exists
}

// GetClusterHealth 获取集群健康状态
func (cm *ClusterMonitor) GetClusterHealth() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// 检查槽分配
	if cm.metrics.UnassignedSlots > 0 {
		return "degraded" // 有未分配的槽
	}

	// 检查节点状态
	for _, nodeMetrics := range cm.metrics.NodeMetrics {
		if nodeMetrics.Status != "connected" {
			return "degraded" // 有节点离线
		}
	}

	// 检查迁移状态
	for _, slotMetrics := range cm.metrics.SlotMetrics {
		if slotMetrics.State == "MIGRATING" || slotMetrics.State == "IMPORTING" {
			return "migrating" // 有槽在迁移
		}
	}

	return "ok" // 集群健康
}

// GetClusterInfo 获取集群信息（CLUSTER INFO 格式）
func (cm *ClusterMonitor) GetClusterInfo() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	info := fmt.Sprintf("cluster_state:%s\n", cm.GetClusterHealth())
	info += fmt.Sprintf("cluster_slots_assigned:%d\n", cm.metrics.AssignedSlots)
	info += fmt.Sprintf("cluster_slots_ok:%d\n", cm.metrics.AssignedSlots)
	info += fmt.Sprintf("cluster_slots_pfail:0\n")
	info += fmt.Sprintf("cluster_slots_fail:0\n")
	info += fmt.Sprintf("cluster_known_nodes:%d\n", cm.metrics.TotalNodes)
	info += fmt.Sprintf("cluster_size:%d\n", cm.metrics.MasterNodes)
	info += fmt.Sprintf("cluster_current_epoch:0\n")
	info += fmt.Sprintf("cluster_my_epoch:0\n")
	info += fmt.Sprintf("cluster_stats_messages_sent:0\n")
	info += fmt.Sprintf("cluster_stats_messages_received:0\n")

	return info
}

// UpdateNodeQPS 更新节点 QPS
func (cm *ClusterMonitor) UpdateNodeQPS(nodeID string, qps float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if metrics, exists := cm.metrics.NodeMetrics[nodeID]; exists {
		metrics.QPS = qps
	}
}

// UpdateNodeLatency 更新节点延迟
func (cm *ClusterMonitor) UpdateNodeLatency(nodeID string, latency float64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if metrics, exists := cm.metrics.NodeMetrics[nodeID]; exists {
		metrics.Latency = latency
	}
}

// IncrementNodeErrors 增加节点错误数
func (cm *ClusterMonitor) IncrementNodeErrors(nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if metrics, exists := cm.metrics.NodeMetrics[nodeID]; exists {
		metrics.Errors++
	}
}

// IncrementNodeRedirects 增加节点重定向数
func (cm *ClusterMonitor) IncrementNodeRedirects(nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if metrics, exists := cm.metrics.NodeMetrics[nodeID]; exists {
		metrics.Redirects++
	}
}
