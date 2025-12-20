package cluster

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

/*
 * ============================================================================
 * Redis 集群槽平衡算法 - Slot Balancing Algorithm
 * ============================================================================
 *
 * 【核心原理】
 * 槽平衡算法用于自动调整槽分配，使数据分布更加均匀，提高集群性能。
 *
 * 1. 平衡目标
 *    - 每个节点分配的槽数量尽量相等
 *    - 每个节点存储的键数量尽量相等
 *    - 考虑节点的性能和容量差异
 *
 * 2. 平衡策略
 *    - 计算当前槽分布
 *    - 识别负载不均衡的节点
 *    - 计算需要迁移的槽
 *    - 执行槽迁移
 *
 * 3. 平衡算法
 *    - 平均分配：将槽平均分配给所有节点
 *    - 加权分配：根据节点性能分配槽
 *    - 动态调整：根据实际负载动态调整
 *
 * 【面试题】
 * Q1: Redis Cluster 的槽平衡算法是什么？
 * A1: 平衡算法：
 *     1. **计算目标分配**：根据节点数量计算每个节点应该分配的槽数
 *     2. **识别不平衡**：找出槽数过多或过少的节点
 *     3. **计算迁移计划**：计算需要迁移的槽和迁移方向
 *     4. **执行迁移**：按计划执行槽迁移
 *     例如：3 个节点，16384 个槽
 *     - 目标：每个节点约 5461 个槽
 *     - 如果 Node1 有 8000 个槽，Node2 有 5000 个槽，Node3 有 3384 个槽
 *     - 需要从 Node1 迁移 2539 个槽到 Node3
 *
 * Q2: 如何考虑节点性能差异进行槽分配？
 * A2: 加权分配策略：
 *     1. **性能评估**：评估每个节点的性能（CPU、内存、网络）
 *     2. **权重计算**：根据性能计算权重
 *     3. **加权分配**：根据权重分配槽
 *     例如：
 *     - Node1: 性能 100，权重 1.0，分配 5461 个槽
 *     - Node2: 性能 150，权重 1.5，分配 8192 个槽
 *     - Node3: 性能 50，权重 0.5，分配 2731 个槽
 *     这样可以充分利用高性能节点
 *
 * Q3: 槽平衡的触发条件是什么？
 * A3: 触发条件：
 *     1. **节点变化**：添加或删除节点
 *     2. **负载不均**：节点间槽数量差异超过阈值（如 10%）
 *     3. **手动触发**：管理员手动触发平衡
 *     4. **定期检查**：定期检查并自动平衡
 *     避免频繁平衡：
 *     - 设置最小间隔时间
 *     - 设置最小差异阈值
 *     - 避免在高峰期平衡
 *
 * Q4: 槽平衡如何避免影响服务？
 * A4: 优化策略：
 *     1. **后台执行**：在后台异步执行平衡
 *     2. **分批迁移**：将迁移分成多个批次
 *     3. **低峰期执行**：在低峰期执行平衡
 *     4. **限流**：限制迁移速度，避免影响正常请求
 *     5. **监控**：监控迁移对性能的影响
 *     6. **可中断**：支持暂停和恢复平衡
 *     这样可以最小化对服务的影响
 *
 * Q5: 如何实现增量槽平衡？
 * A5: 增量平衡策略：
 *     1. **识别差异**：计算当前分配与目标分配的差异
 *     2. **选择槽**：选择负载最重的节点中的槽
 *     3. **选择目标**：选择负载最轻的节点作为目标
 *     4. **逐步迁移**：每次迁移少量槽，逐步达到平衡
 *     优点：
 *     - 对服务影响小
 *     - 可以随时中断
 *     - 可以监控效果
 *     缺点：
 *     - 平衡时间较长
 *     - 需要持续监控
 *
 * Q6: 槽平衡时如何选择要迁移的槽？
 * A6: 选择策略：
 *     1. **随机选择**：随机选择槽迁移（简单但可能不均匀）
 *     2. **负载选择**：选择键数量最多的槽（更有效）
 *     3. **范围选择**：选择连续的槽范围（便于管理）
 *     4. **混合策略**：结合多种策略
 *     推荐策略：
 *     - 优先迁移键数量多的槽
 *     - 避免迁移正在使用的热点槽
 *     - 考虑槽的访问模式
 */

// BalancingStrategy 平衡策略
type BalancingStrategy int

const (
	BALANCE_EQUAL    BalancingStrategy = iota // 平均分配
	BALANCE_WEIGHTED                          // 加权分配
	BALANCE_ADAPTIVE                          // 自适应分配
)

// NodeWeight 节点权重
type NodeWeight struct {
	NodeID string
	Weight float64 // 权重（基于性能）
}

// BalancingPlan 平衡计划
type BalancingPlan struct {
	Migrations []*MigrationPlan `json:"migrations"`
	TotalSlots int              `json:"total_slots"`
	StartTime  int64            `json:"start_time"`
	EndTime    int64            `json:"end_time"`
}

// MigrationPlan 迁移计划
type MigrationPlan struct {
	Slot         int    `json:"slot"`
	SourceNodeID string `json:"source_node_id"`
	TargetNodeID string `json:"target_node_id"`
	Priority     int    `json:"priority"` // 优先级（1-10，10 最高）
}

// SlotBalancer 槽平衡器
type SlotBalancer struct {
	cluster     *Cluster
	strategy    BalancingStrategy
	nodeWeights map[string]float64 // nodeID -> weight
	mu          sync.RWMutex
	threshold   float64 // 不平衡阈值（百分比）
}

// NewSlotBalancer 创建槽平衡器
func NewSlotBalancer(cluster *Cluster) *SlotBalancer {
	return &SlotBalancer{
		cluster:     cluster,
		strategy:    BALANCE_EQUAL,
		nodeWeights: make(map[string]float64),
		threshold:   10.0, // 10% 差异阈值
	}
}

// SetStrategy 设置平衡策略
func (sb *SlotBalancer) SetStrategy(strategy BalancingStrategy) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.strategy = strategy
}

// SetNodeWeight 设置节点权重
func (sb *SlotBalancer) SetNodeWeight(nodeID string, weight float64) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.nodeWeights[nodeID] = weight
}

// CalculateBalancePlan 计算平衡计划
func (sb *SlotBalancer) CalculateBalancePlan() (*BalancingPlan, error) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	plan := &BalancingPlan{
		Migrations: make([]*MigrationPlan, 0),
		TotalSlots: CLUSTER_SLOTS,
		StartTime:  getCurrentTimestamp(),
	}

	// 获取所有主节点
	masterNodes := sb.getMasterNodes()
	if len(masterNodes) == 0 {
		return nil, errors.New("no master nodes")
	}

	// 根据策略计算目标分配
	targetSlots := sb.calculateTargetSlots(masterNodes)

	// 计算当前分配
	currentSlots := sb.calculateCurrentSlots(masterNodes)

	// 计算需要迁移的槽
	migrations := sb.calculateMigrations(currentSlots, targetSlots, masterNodes)

	plan.Migrations = migrations

	return plan, nil
}

// getMasterNodes 获取所有主节点
func (sb *SlotBalancer) getMasterNodes() []*ClusterNode {
	nodes := sb.cluster.GetNodes()
	masters := make([]*ClusterNode, 0)

	for _, node := range nodes {
		if node.Master == nil {
			masters = append(masters, node)
		}
	}

	return masters
}

// calculateTargetSlots 计算目标槽分配
func (sb *SlotBalancer) calculateTargetSlots(nodes []*ClusterNode) map[string]int {
	targetSlots := make(map[string]int)

	switch sb.strategy {
	case BALANCE_EQUAL:
		// 平均分配
		slotsPerNode := CLUSTER_SLOTS / len(nodes)
		remainder := CLUSTER_SLOTS % len(nodes)

		for i, node := range nodes {
			targetSlots[node.NodeID] = slotsPerNode
			if i < remainder {
				targetSlots[node.NodeID]++
			}
		}

	case BALANCE_WEIGHTED:
		// 加权分配
		totalWeight := 0.0
		for _, node := range nodes {
			weight := sb.nodeWeights[node.NodeID]
			if weight == 0 {
				weight = 1.0 // 默认权重
			}
			totalWeight += weight
		}

		allocated := 0
		for _, node := range nodes {
			weight := sb.nodeWeights[node.NodeID]
			if weight == 0 {
				weight = 1.0
			}

			slots := int(float64(CLUSTER_SLOTS) * weight / totalWeight)
			targetSlots[node.NodeID] = slots
			allocated += slots
		}

		// 分配剩余的槽
		remaining := CLUSTER_SLOTS - allocated
		for idx := 0; idx < remaining && idx < len(nodes); idx++ {
			targetSlots[nodes[idx].NodeID]++
		}

	case BALANCE_ADAPTIVE:
		// 自适应分配（基于当前负载）
		// 简化实现：使用平均分配
		return sb.calculateTargetSlotsWithStrategy(nodes, BALANCE_EQUAL)
	}

	return targetSlots
}

// calculateTargetSlotsWithStrategy 使用指定策略计算目标槽
func (sb *SlotBalancer) calculateTargetSlotsWithStrategy(nodes []*ClusterNode, strategy BalancingStrategy) map[string]int {
	oldStrategy := sb.strategy
	sb.strategy = strategy
	result := sb.calculateTargetSlots(nodes)
	sb.strategy = oldStrategy
	return result
}

// calculateCurrentSlots 计算当前槽分配
func (sb *SlotBalancer) calculateCurrentSlots(nodes []*ClusterNode) map[string]int {
	currentSlots := make(map[string]int)

	for _, node := range nodes {
		currentSlots[node.NodeID] = len(node.Slots)
	}

	return currentSlots
}

// calculateMigrations 计算需要迁移的槽
func (sb *SlotBalancer) calculateMigrations(currentSlots, targetSlots map[string]int, nodes []*ClusterNode) []*MigrationPlan {
	migrations := make([]*MigrationPlan, 0)

	// 找出需要减少槽的节点（源节点）
	sources := make([]*ClusterNode, 0)
	for _, node := range nodes {
		current := currentSlots[node.NodeID]
		target := targetSlots[node.NodeID]
		if current > target {
			sources = append(sources, node)
		}
	}

	// 找出需要增加槽的节点（目标节点）
	targets := make([]*ClusterNode, 0)
	for _, node := range nodes {
		current := currentSlots[node.NodeID]
		target := targetSlots[node.NodeID]
		if current < target {
			targets = append(targets, node)
		}
	}

	// 按差异排序（优先处理差异大的）
	sort.Slice(sources, func(i, j int) bool {
		diffI := currentSlots[sources[i].NodeID] - targetSlots[sources[i].NodeID]
		diffJ := currentSlots[sources[j].NodeID] - targetSlots[sources[j].NodeID]
		return diffI > diffJ
	})

	sort.Slice(targets, func(i, j int) bool {
		diffI := targetSlots[targets[i].NodeID] - currentSlots[targets[i].NodeID]
		diffJ := targetSlots[targets[j].NodeID] - currentSlots[targets[j].NodeID]
		return diffI > diffJ
	})

	// 分配迁移任务
	sourceIdx := 0
	targetIdx := 0

	for sourceIdx < len(sources) && targetIdx < len(targets) {
		source := sources[sourceIdx]
		target := targets[targetIdx]

		sourceCurrent := currentSlots[source.NodeID]
		sourceTarget := targetSlots[source.NodeID]
		targetCurrent := currentSlots[target.NodeID]
		targetTarget := targetSlots[target.NodeID]

		// 计算需要迁移的槽数
		sourceNeed := sourceCurrent - sourceTarget
		targetNeed := targetTarget - targetCurrent
		migrateCount := min(sourceNeed, targetNeed)

		// 选择要迁移的槽
		slotsToMigrate := sb.selectSlotsToMigrate(source, migrateCount)

		// 创建迁移计划
		for _, slot := range slotsToMigrate {
			migration := &MigrationPlan{
				Slot:         slot,
				SourceNodeID: source.NodeID,
				TargetNodeID: target.NodeID,
				Priority:     5, // 默认优先级
			}
			migrations = append(migrations, migration)
		}

		// 更新当前分配
		currentSlots[source.NodeID] -= migrateCount
		currentSlots[target.NodeID] += migrateCount

		// 检查是否完成
		if currentSlots[source.NodeID] == targetSlots[source.NodeID] {
			sourceIdx++
		}
		if currentSlots[target.NodeID] == targetSlots[target.NodeID] {
			targetIdx++
		}
	}

	return migrations
}

// selectSlotsToMigrate 选择要迁移的槽
func (sb *SlotBalancer) selectSlotsToMigrate(node *ClusterNode, count int) []int {
	// 简化实现：随机选择槽
	// 实际应该考虑槽的负载（键数量）
	slots := make([]int, 0, count)

	for i := 0; i < count && i < len(node.Slots); i++ {
		slots = append(slots, node.Slots[i])
	}

	return slots
}

// ExecuteBalancePlan 执行平衡计划
func (sb *SlotBalancer) ExecuteBalancePlan(plan *BalancingPlan) error {
	if plan == nil || len(plan.Migrations) == 0 {
		return errors.New("empty balance plan")
	}

	reshardingMgr := sb.cluster.GetReshardingManager()
	if reshardingMgr == nil {
		return errors.New("resharding manager not available")
	}

	// 按优先级排序
	sort.Slice(plan.Migrations, func(i, j int) bool {
		return plan.Migrations[i].Priority > plan.Migrations[j].Priority
	})

	// 执行迁移
	for _, migration := range plan.Migrations {
		// 开始迁移
		if err := reshardingMgr.StartMigration(migration.Slot, migration.SourceNodeID, migration.TargetNodeID); err != nil {
			fmt.Printf("Failed to start migration for slot %d: %v\n", migration.Slot, err)
			continue
		}

		// 执行迁移（与存储层集成）
		if sb.cluster.server != nil {
			if err := reshardingMgr.MigrateSlotData(migration.Slot, migration.SourceNodeID, migration.TargetNodeID, sb.cluster.server); err != nil {
				fmt.Printf("Failed to migrate slot %d: %v\n", migration.Slot, err)
				// 取消迁移
				reshardingMgr.CancelMigration(migration.Slot)
				continue
			}
		} else {
			// 简化实现（无存储层）
			if err := reshardingMgr.MigrateSlot(migration.Slot, migration.SourceNodeID, migration.TargetNodeID); err != nil {
				fmt.Printf("Failed to migrate slot %d: %v\n", migration.Slot, err)
				// 取消迁移
				reshardingMgr.CancelMigration(migration.Slot)
				continue
			}
		}
	}

	plan.EndTime = getCurrentTimestamp()
	return nil
}

// CheckBalance 检查是否需要平衡
func (sb *SlotBalancer) CheckBalance() (bool, float64) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	masterNodes := sb.getMasterNodes()
	if len(masterNodes) == 0 {
		return false, 0
	}

	// 计算槽数量
	slotCounts := make([]int, 0, len(masterNodes))
	totalSlots := 0

	for _, node := range masterNodes {
		count := len(node.Slots)
		slotCounts = append(slotCounts, count)
		totalSlots += count
	}

	if len(slotCounts) == 0 {
		return false, 0
	}

	// 计算平均值
	avgSlots := float64(totalSlots) / float64(len(slotCounts))

	// 计算最大差异
	maxDiff := 0.0
	for _, count := range slotCounts {
		diff := math.Abs(float64(count) - avgSlots)
		if diff > maxDiff {
			maxDiff = diff
		}
	}

	// 计算差异百分比
	diffPercent := (maxDiff / avgSlots) * 100.0

	// 检查是否超过阈值
	needBalance := diffPercent > sb.threshold

	return needBalance, diffPercent
}

// AutoBalance 自动平衡
func (sb *SlotBalancer) AutoBalance() error {
	// 检查是否需要平衡
	needBalance, diffPercent := sb.CheckBalance()
	if !needBalance {
		return fmt.Errorf("cluster is balanced (difference: %.2f%%)", diffPercent)
	}

	// 计算平衡计划
	plan, err := sb.CalculateBalancePlan()
	if err != nil {
		return err
	}

	// 执行平衡
	return sb.ExecuteBalancePlan(plan)
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
