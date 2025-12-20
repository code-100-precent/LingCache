package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 集群槽迁移实现 - Slot Resharding
 * ============================================================================
 *
 * 【核心原理】
 * 槽迁移（Resharding）是 Redis Cluster 动态调整数据分布的关键功能。
 * 当需要添加/删除节点或重新平衡数据时，需要将槽从一个节点迁移到另一个节点。
 *
 * 1. 迁移状态
 *    - NORMAL: 槽正常状态，由某个节点负责
 *    - MIGRATING: 槽正在从源节点迁移出去
 *    - IMPORTING: 槽正在导入到目标节点
 *    - STABLE: 迁移完成，槽已稳定
 *
 * 2. 迁移流程
 *    a) 准备阶段：
 *       - 目标节点：CLUSTER SETSLOT <slot> IMPORTING <source-node-id>
 *       - 源节点：CLUSTER SETSLOT <slot> MIGRATING <target-node-id>
 *    b) 迁移阶段：
 *       - 对槽中的每个键，执行 MIGRATE 命令
 *       - 迁移过程中，源节点仍可处理读请求
 *       - 写请求会收到 ASK 重定向，转发到目标节点
 *    c) 完成阶段：
 *       - 所有节点更新槽分配：CLUSTER SETSLOT <slot> NODE <target-node-id>
 *       - 源节点删除已迁移的键
 *       - 清理迁移状态
 *
 * 3. MIGRATE 命令
 *    MIGRATE <host> <port> <key> <destination-db> <timeout> [COPY] [REPLACE]
 *    - 原子性地将键从源节点迁移到目标节点
 *    - 如果键不存在，返回 NIL
 *    - 迁移成功后，源节点删除该键
 *
 * 4. 迁移过程中的请求处理
 *    - 读请求：源节点处理（键还在源节点）
 *    - 写请求：
 *      * 如果键在源节点：返回 ASK 重定向
 *      * 如果键在目标节点：目标节点处理
 *      * 如果键不存在：目标节点创建（因为迁移已完成）
 *
 * 【面试题】
 * Q1: Redis Cluster 的槽迁移为什么需要两个状态（MIGRATING 和 IMPORTING）？
 * A1: 两个状态分别表示源节点和目标节点的迁移状态：
 *     - MIGRATING（源节点）：表示槽正在从该节点迁移出去
 *       * 读请求：正常处理（键还在源节点）
 *       * 写请求：如果键存在，返回 ASK 重定向到目标节点
 *     - IMPORTING（目标节点）：表示槽正在导入到该节点
 *       * 读请求：如果键不存在，返回 ASK 重定向到源节点
 *       * 写请求：正常处理（允许创建新键）
 *     这样可以保证迁移过程中数据的一致性和可用性
 *
 * Q2: MIGRATE 命令和 ASK 重定向有什么区别？
 * A2: 主要区别：
 *     - **MIGRATE**：服务端命令，用于实际迁移键值对
 *       * 原子性操作：键从源节点删除，在目标节点创建
 *       * 由集群管理工具或脚本调用
 *     - **ASK 重定向**：客户端重定向机制
 *       * 临时重定向：只针对当前请求
 *       * 不更新客户端缓存：客户端仍使用原节点路由
 *       * 用于迁移过程中的请求转发
 *     MOVED vs ASK：
 *     - MOVED：槽已永久迁移，更新客户端缓存
 *     - ASK：槽正在迁移，临时重定向，不更新缓存
 *
 * Q3: 槽迁移过程中如何保证数据一致性？
 * A3: 一致性保证机制：
 *     1. **原子性迁移**：MIGRATE 命令是原子的，键要么在源节点，要么在目标节点
 *     2. **写请求重定向**：迁移中的键，写请求通过 ASK 重定向到目标节点
 *     3. **顺序保证**：迁移按键的顺序进行，避免并发问题
 *     4. **最终一致性**：迁移完成后，所有节点更新槽分配
 *     可能的问题：
 *     - 迁移过程中，如果源节点故障，可能丢失正在迁移的键
 *     - 需要确保迁移的原子性和幂等性
 *
 * Q4: 槽迁移的性能影响是什么？
 * A4: 性能影响：
 *     - **网络开销**：需要传输键值对数据，大键会影响性能
 *     - **CPU 开销**：序列化/反序列化数据
 *     - **内存开销**：迁移过程中，键可能同时存在于两个节点
 *     优化策略：
 *     1. 批量迁移：一次迁移多个键
 *     2. 后台迁移：在低峰期进行迁移
 *     3. 增量迁移：只迁移变更的键
 *     4. 并行迁移：多个槽并行迁移（需要谨慎）
 *
 * Q5: 如何实现槽的批量迁移？
 * A5: 批量迁移流程：
 *     1. 扫描槽中的所有键：使用 SCAN 命令遍历
 *     2. 批量获取键值：使用 MGET 或管道批量获取
 *     3. 批量迁移：使用管道批量执行 MIGRATE
 *     4. 验证迁移：检查源节点和目标节点的键数量
 *     5. 更新槽分配：迁移完成后更新所有节点
 *     注意事项：
 *     - 需要处理大键（超过网络包大小）
 *     - 需要处理迁移失败的情况
 *     - 需要监控迁移进度
 *
 * Q6: 槽迁移失败如何处理？
 * A6: 失败处理机制：
 *     1. **部分迁移失败**：
 *        * 记录失败的键
 *        * 重试失败的键
 *        * 如果重试多次仍失败，回滚迁移
 *     2. **节点故障**：
 *        * 如果源节点故障：已迁移的键在目标节点，未迁移的键丢失
 *        * 如果目标节点故障：键仍在源节点，需要重新迁移
 *     3. **网络中断**：
 *        * 检测迁移状态
 *        * 恢复迁移：从未完成的键继续
 *     4. **回滚机制**：
 *        * 将已迁移的键迁移回源节点
 *        * 恢复槽分配状态
 */

// SlotState 槽状态
type SlotState int

const (
	SLOT_STATE_NORMAL    SlotState = iota // 正常状态
	SLOT_STATE_MIGRATING                  // 正在迁移出去
	SLOT_STATE_IMPORTING                  // 正在导入
	SLOT_STATE_STABLE                     // 迁移完成
)

// SlotMigration 槽迁移信息
type SlotMigration struct {
	Slot         int
	SourceNodeID string
	TargetNodeID string
	State        SlotState
	StartTime    time.Time
	EndTime      time.Time
	KeysMigrated int
	KeysTotal    int
}

// ReshardingManager 槽迁移管理器
type ReshardingManager struct {
	cluster        *Cluster
	migrations     map[int]*SlotMigration // slot -> migration
	migratingSlots map[int]bool           // slot -> is migrating
	importingSlots map[int]string         // slot -> source node ID
	mu             sync.RWMutex
}

// NewReshardingManager 创建槽迁移管理器
func NewReshardingManager(cluster *Cluster) *ReshardingManager {
	return &ReshardingManager{
		cluster:        cluster,
		migrations:     make(map[int]*SlotMigration),
		migratingSlots: make(map[int]bool),
		importingSlots: make(map[int]string),
	}
}

// StartMigration 开始槽迁移
func (rm *ReshardingManager) StartMigration(slot int, sourceNodeID, targetNodeID string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 检查槽是否已经在迁移
	if rm.migratingSlots[slot] {
		return errors.New("slot is already migrating")
	}

	// 检查目标节点是否存在
	rm.cluster.mu.RLock()
	_, exists := rm.cluster.nodes[targetNodeID]
	rm.cluster.mu.RUnlock()
	if !exists {
		return errors.New("target node not found")
	}

	// 创建迁移记录
	migration := &SlotMigration{
		Slot:         slot,
		SourceNodeID: sourceNodeID,
		TargetNodeID: targetNodeID,
		State:        SLOT_STATE_MIGRATING,
		StartTime:    time.Now(),
		KeysMigrated: 0,
		KeysTotal:    0,
	}

	rm.migrations[slot] = migration
	rm.migratingSlots[slot] = true
	rm.importingSlots[slot] = sourceNodeID

	// 设置槽状态
	// 源节点：MIGRATING
	// 目标节点：IMPORTING
	// 这里简化实现，实际应该通过网络协议通知节点

	return nil
}

// IsSlotMigrating 检查槽是否正在迁移
func (rm *ReshardingManager) IsSlotMigrating(slot int) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.migratingSlots[slot]
}

// IsSlotImporting 检查槽是否正在导入
func (rm *ReshardingManager) IsSlotImporting(slot int) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	_, exists := rm.importingSlots[slot]
	return exists
}

// GetImportingSourceNode 获取导入槽的源节点
func (rm *ReshardingManager) GetImportingSourceNode(slot int) (string, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	sourceNodeID, exists := rm.importingSlots[slot]
	return sourceNodeID, exists
}

// MigrateKey 迁移单个键
func (rm *ReshardingManager) MigrateKey(key string, targetNode *ClusterNode, timeout time.Duration) error {
	// 获取键的值
	slot := HashSlot(key)

	// 检查槽是否正在迁移
	if !rm.IsSlotMigrating(slot) {
		return errors.New("slot is not migrating")
	}

	// 从源节点获取键值
	// 这里简化实现，实际应该通过 MIGRATE 命令迁移
	// MIGRATE <host> <port> <key> <destination-db> <timeout> [COPY] [REPLACE]

	// 模拟迁移过程
	migration, exists := rm.migrations[slot]
	if !exists {
		return errors.New("migration not found")
	}

	// 这里应该实际执行 MIGRATE 命令
	// 简化实现：直接更新迁移进度
	migration.KeysMigrated++

	return nil
}

// CompleteMigration 完成槽迁移
func (rm *ReshardingManager) CompleteMigration(slot int) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	migration, exists := rm.migrations[slot]
	if !exists {
		return errors.New("migration not found")
	}

	// 更新槽分配
	rm.cluster.AssignSlots(migration.TargetNodeID, []int{slot})

	// 清理迁移状态
	migration.State = SLOT_STATE_STABLE
	migration.EndTime = time.Now()
	delete(rm.migratingSlots, slot)
	delete(rm.importingSlots, slot)

	// 通知所有节点更新槽分配
	rm.notifySlotUpdate(slot, migration.TargetNodeID)

	return nil
}

// notifySlotUpdate 通知节点更新槽分配
func (rm *ReshardingManager) notifySlotUpdate(slot int, nodeID string) {
	// 通过 Gossip 协议通知所有节点
	// 简化实现：更新本地集群状态
	// 实际应该通过 communicator 发送 SLOTS 消息
}

// GetMigrationStatus 获取迁移状态
func (rm *ReshardingManager) GetMigrationStatus(slot int) (*SlotMigration, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	migration, exists := rm.migrations[slot]
	return migration, exists
}

// GetAllMigrations 获取所有迁移
func (rm *ReshardingManager) GetAllMigrations() []*SlotMigration {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	migrations := make([]*SlotMigration, 0, len(rm.migrations))
	for _, migration := range rm.migrations {
		migrations = append(migrations, migration)
	}
	return migrations
}

// MigrateSlot 迁移整个槽（批量迁移）
func (rm *ReshardingManager) MigrateSlot(slot int, sourceNodeID, targetNodeID string) error {
	// 开始迁移
	if err := rm.StartMigration(slot, sourceNodeID, targetNodeID); err != nil {
		return err
	}

	// 检查目标节点是否存在
	rm.cluster.mu.RLock()
	targetNode, exists := rm.cluster.nodes[targetNodeID]
	rm.cluster.mu.RUnlock()
	if !exists {
		return errors.New("target node not found")
	}

	// 获取槽中的所有键
	// 这里简化实现，实际应该通过 SCAN 命令遍历槽中的键
	keys := rm.getKeysInSlot(slot)

	// 迁移每个键
	for _, key := range keys {
		if err := rm.MigrateKey(key, targetNode, 5*time.Second); err != nil {
			// 记录失败，继续迁移其他键
			fmt.Printf("Failed to migrate key %s: %v\n", key, err)
		}
	}

	// 完成迁移
	return rm.CompleteMigration(slot)
}

// getKeysInSlot 获取槽中的所有键（简化实现）
func (rm *ReshardingManager) getKeysInSlot(slot int) []string {
	// 实际应该通过 SCAN 命令遍历数据库，找到槽中的所有键
	// 这里返回空，需要实际实现
	return []string{}
}

// CancelMigration 取消迁移
func (rm *ReshardingManager) CancelMigration(slot int) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	_, exists := rm.migrations[slot]
	if !exists {
		return errors.New("migration not found")
	}

	// 清理迁移状态
	delete(rm.migrations, slot)
	delete(rm.migratingSlots, slot)
	delete(rm.importingSlots, slot)

	// 如果已迁移部分键，需要回滚
	// 这里简化实现，实际应该将已迁移的键迁移回源节点

	return nil
}

// GetMigrationProgress 获取迁移进度
func (rm *ReshardingManager) GetMigrationProgress(slot int) (int, int, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	migration, exists := rm.migrations[slot]
	if !exists {
		return 0, 0, errors.New("migration not found")
	}

	return migration.KeysMigrated, migration.KeysTotal, nil
}

// SerializeMigration 序列化迁移信息（用于持久化）
func (m *SlotMigration) SerializeMigration() ([]byte, error) {
	return json.Marshal(m)
}

// DeserializeMigration 反序列化迁移信息
func DeserializeMigration(data []byte) (*SlotMigration, error) {
	var migration SlotMigration
	if err := json.Unmarshal(data, &migration); err != nil {
		return nil, err
	}
	return &migration, nil
}
