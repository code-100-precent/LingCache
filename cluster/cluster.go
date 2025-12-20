package cluster

import (
	"fmt"
	"sync"

	"github.com/code-100-precent/LingCache/storage"
)

/*
 * ============================================================================
 * Redis 集群模式实现 - Cluster Mode
 * ============================================================================
 *
 * 【核心原理】
 * Redis Cluster 是 Redis 的分布式实现，通过分片（Sharding）将数据分布到多个节点。
 *
 * 1. 哈希槽（Hash Slot）分片
 *    - 共有 16384 个哈希槽（0-16383）
 *    - 每个键通过 CRC16 算法计算槽号：slot = CRC16(key) % 16384
 *    - 每个节点负责一部分槽，槽可以动态迁移
 *
 * 2. 无中心化架构
 *    - 没有中心节点，所有节点地位平等
 *    - 节点间通过 Gossip 协议通信
 *    - 每个节点维护完整的集群拓扑信息
 *
 * 3. 高可用性
 *    - 主从复制：每个主节点可以有多个从节点
 *    - 故障转移：主节点故障时，从节点自动提升为主节点
 *    - 自动故障检测：通过心跳机制检测节点状态
 *
 * 【哈希槽分配】
 * 假设有 3 个节点：
 * - Node1: 0-5460 (5461 个槽)
 * - Node2: 5461-10922 (5462 个槽)
 * - Node3: 10923-16383 (5461 个槽)
 *
 * 键 "user:1000" 的槽号计算：
 *   slot = CRC16("user:1000") % 16384
 *   假设结果是 5000，则路由到 Node1
 *
 * 【键的哈希标签（Hash Tag）】
 * 使用 {} 指定哈希标签，只有标签内的内容参与哈希计算：
 * - "user:{1000}:profile" → 只计算 "1000" 的哈希
 * - "user:{1000}:settings" → 只计算 "1000" 的哈希
 * 这样可以将相关键路由到同一个节点，支持多键操作
 *
 * 【面试题】
 * Q1: 为什么 Redis Cluster 使用 16384 个槽，而不是更多或更少？
 * A1: 16384 是一个平衡的选择：
 *     - 槽太少（如 1024）：每个节点分配的槽范围太大，数据分布不均匀
 *     - 槽太多（如 65536）：节点间通信开销大（需要传输更多槽信息）
 *     16384 的好处：
 *     - 每个节点平均分配约 5461 个槽（3节点）或 2730 个槽（6节点）
 *     - 槽信息可以用 2KB 传输（16384/8 = 2048 字节）
 *     - 在数据分布和通信开销之间取得平衡
 *
 * Q2: Redis Cluster 如何保证数据一致性？
 * A2: Redis Cluster 采用最终一致性模型：
 *     - 主从异步复制：主节点写入后异步复制到从节点
 *     - 可能丢失数据：主节点故障时，未复制的数据会丢失
 *     - 强一致性选项：可以使用 WAIT 命令等待复制完成
 *     如果需要强一致性，建议使用 Redis Sentinel 或外部一致性方案
 *
 * Q3: Redis Cluster 的槽迁移（Resharding）是如何工作的？
 * A3: 槽迁移过程：
 *     1. 目标节点准备接收槽：CLUSTER SETSLOT <slot> IMPORTING <source-node-id>
 *     2. 源节点准备迁移槽：CLUSTER SETSLOT <slot> MIGRATING <target-node-id>
 *     3. 迁移键值对：对槽中的每个键，执行 MIGRATE 命令
 *     4. 更新槽分配：所有节点更新槽到新节点的映射
 *     5. 清理：源节点删除已迁移的键
 *     迁移过程中，客户端会收到 ASK 重定向，自动重试到新节点
 *
 * Q4: Redis Cluster 如何处理跨槽操作（Multi-key Operations）？
 * A4: 跨槽操作的限制：
 *     - 不支持跨槽的批量操作（如 MGET、MSET）
 *     - 不支持跨槽的事务（MULTI/EXEC）
 *     解决方案：
 *     1. 使用哈希标签（Hash Tag）：将相关键路由到同一节点
 *        例如：{user:1000}:profile 和 {user:1000}:settings
 *     2. 使用 Lua 脚本：Lua 脚本在单个节点执行，可以操作多个键
 *     3. 客户端分片：客户端将操作拆分到不同节点
 *
 * Q5: Redis Cluster 的故障转移是如何触发的？
 * A5: 故障转移流程：
 *     1. 心跳检测：每个节点定期向其他节点发送 PING
 *     2. 故障判定：如果主节点在指定时间内（默认 15 秒）没有响应 PONG
 *     3. 投票机制：其他主节点投票决定是否认为该节点故障
 *     4. 从节点选举：故障主节点的从节点中，选举新的主节点
 *     5. 槽转移：将原主节点的槽转移到新主节点
 *     6. 通知更新：通知所有节点更新集群拓扑
 *
 * Q6: Redis Cluster 和 Redis Sentinel 有什么区别？
 * A6: 主要区别：
 *     - **架构**：
 *       * Cluster：无中心化，所有节点地位平等
 *       * Sentinel：中心化，Sentinel 节点监控主从节点
 *     - **分片**：
 *       * Cluster：自动分片，数据分布到多个节点
 *       * Sentinel：不分片，所有数据在一个主节点
 *     - **扩展性**：
 *       * Cluster：水平扩展，可以增加节点
 *       * Sentinel：垂直扩展，只能增加从节点
 *     - **使用场景**：
 *       * Cluster：大数据量、需要分片
 *       * Sentinel：小数据量、高可用性
 *
 * Q7: Redis Cluster 的 Gossip 协议是什么？
 * A7: Gossip 协议是一种去中心化的通信协议：
 *     - 每个节点定期随机选择其他节点交换信息
 *     - 信息包括：节点状态、槽分配、故障信息等
 *     - 优点：去中心化、容错性强、扩展性好
 *     - 缺点：信息传播有延迟，可能不一致
 *     Redis Cluster 使用 Gossip 协议维护集群拓扑
 *
 * Q8: Redis Cluster 如何处理网络分区（Split-brain）？
 * A8: Redis Cluster 通过多数派原则防止脑裂：
 *     - 当网络分区时，只有拥有大多数主节点的分区可以继续服务
 *     - 少数派分区会停止接受写请求
 *     - 例如：6 个主节点，分区为 3-3，两个分区都不能服务
 *     这样可以保证数据一致性，但可能降低可用性
 *
 * Q9: Redis Cluster 的客户端如何路由请求？
 * A9: 客户端路由流程：
 *     1. 计算槽号：slot = CRC16(key) % 16384
 *     2. 查找节点：从本地缓存的槽-节点映射中查找
 *     3. 发送请求：向对应节点发送命令
 *     4. 处理重定向：
 *        * MOVED <slot> <node>：槽已迁移，更新缓存
 *        * ASK <slot> <node>：槽正在迁移，临时重定向
 *     5. 更新缓存：根据重定向信息更新本地缓存
 *
 * Q10: Redis Cluster 的槽分配策略是什么？
 * A10: 槽分配原则：
 *      - 均匀分配：尽量让每个节点分配的槽数量相近
 *      - 手动分配：通过 CLUSTER ADDSLOTS 手动分配
 *      - 自动平衡：可以使用工具自动重新分配槽
 *      例如 3 节点：
 *      - Node1: 0-5460 (5461 个槽)
 *      - Node2: 5461-10922 (5462 个槽)
 *      - Node3: 10923-16383 (5461 个槽)
 */

const (
	CLUSTER_SLOTS = 16384 // Redis 集群槽数
)

// ClusterNode 集群节点
type ClusterNode struct {
	NodeID   string
	Addr     string
	Slots    []int // 负责的槽
	Master   *ClusterNode
	Replicas []*ClusterNode
}

// Cluster 集群
type Cluster struct {
	nodes             map[string]*ClusterNode     // nodeID -> node
	slots             [CLUSTER_SLOTS]*ClusterNode // slot -> node
	myself            *ClusterNode
	failoverMgr       *FailoverManager
	communicator      *NodeCommunicator
	reshardingMgr     *ReshardingManager
	configPersistence *ConfigPersistence
	monitor           *ClusterMonitor
	balancer          *SlotBalancer
	robustComm        *RobustNodeCommunicator
	mu                sync.RWMutex
	server            *storage.RedisServer
}

// NewCluster 创建集群
func NewCluster(server *storage.RedisServer, nodeID string, addr string) *Cluster {
	myself := &ClusterNode{
		NodeID: nodeID,
		Addr:   addr,
		Slots:  make([]int, 0),
	}

	cluster := &Cluster{
		nodes:  make(map[string]*ClusterNode),
		slots:  [CLUSTER_SLOTS]*ClusterNode{},
		myself: myself,
		server: server,
	}

	// 初始化故障转移管理器和通信器
	cluster.failoverMgr = NewFailoverManager(cluster)
	cluster.communicator = NewNodeCommunicator(cluster)
	cluster.reshardingMgr = NewReshardingManager(cluster)
	cluster.configPersistence = NewConfigPersistence(cluster, "nodes.conf")
	cluster.monitor = NewClusterMonitor(cluster)
	cluster.balancer = NewSlotBalancer(cluster)
	cluster.robustComm = NewRobustNodeCommunicator(cluster)

	return cluster
}

// AddNode 添加节点
func (c *Cluster) AddNode(nodeID string, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodes[nodeID]; !exists {
		c.nodes[nodeID] = &ClusterNode{
			NodeID: nodeID,
			Addr:   addr,
			Slots:  make([]int, 0),
		}
	}
}

// AssignSlots 分配槽给节点
func (c *Cluster) AssignSlots(nodeID string, slots []int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.nodes[nodeID]
	if !exists {
		return
	}

	for _, slot := range slots {
		if slot >= 0 && slot < CLUSTER_SLOTS {
			c.slots[slot] = node
			node.Slots = append(node.Slots, slot)
		}
	}
}

// GetSlotNode 获取负责指定槽的节点
func (c *Cluster) GetSlotNode(slot int) *ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if slot >= 0 && slot < CLUSTER_SLOTS {
		return c.slots[slot]
	}
	return nil
}

// crc16Table CRC16 查找表
var crc16Table = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd33c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

// crc16 计算 CRC16 校验和
func crc16(data []byte) uint16 {
	var crc uint16 = 0
	for _, b := range data {
		crc = (crc << 8) ^ crc16Table[(crc>>8)^uint16(b)]
	}
	return crc
}

// HashSlot 计算键的槽号
func HashSlot(key string) int {
	// 提取键中的 {} 部分（如果有）
	start := -1
	end := -1
	for i, c := range key {
		if c == '{' {
			start = i
		} else if c == '}' && start != -1 {
			end = i
			break
		}
	}

	var hashKey string
	if start != -1 && end != -1 && end > start+1 {
		hashKey = key[start+1 : end]
	} else {
		hashKey = key
	}

	// 使用 CRC16 算法
	hash := crc16([]byte(hashKey))
	return int(hash) % CLUSTER_SLOTS
}

// GetNodeForKey 获取键对应的节点
func (c *Cluster) GetNodeForKey(key string) *ClusterNode {
	slot := HashSlot(key)
	return c.GetSlotNode(slot)
}

// GetNodes 获取所有节点
func (c *Cluster) GetNodes() []*ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*ClusterNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetMyself 获取当前节点
func (c *Cluster) GetMyself() *ClusterNode {
	return c.myself
}

// GetFailoverManager 获取故障转移管理器
func (c *Cluster) GetFailoverManager() *FailoverManager {
	return c.failoverMgr
}

// GetCommunicator 获取通信器
func (c *Cluster) GetCommunicator() *NodeCommunicator {
	return c.communicator
}

// GetReshardingManager 获取槽迁移管理器
func (c *Cluster) GetReshardingManager() *ReshardingManager {
	return c.reshardingMgr
}

// GetConfigPersistence 获取配置持久化管理器
func (c *Cluster) GetConfigPersistence() *ConfigPersistence {
	return c.configPersistence
}

// GetMonitor 获取集群监控器
func (c *Cluster) GetMonitor() *ClusterMonitor {
	return c.monitor
}

// GetBalancer 获取槽平衡器
func (c *Cluster) GetBalancer() *SlotBalancer {
	return c.balancer
}

// GetRobustCommunicator 获取健壮通信器
func (c *Cluster) GetRobustCommunicator() *RobustNodeCommunicator {
	return c.robustComm
}

// SaveConfig 保存集群配置
func (c *Cluster) SaveConfig() error {
	return c.configPersistence.SaveConfig()
}

// LoadConfig 加载集群配置
func (c *Cluster) LoadConfig() error {
	return c.configPersistence.LoadConfig()
}

// Start 启动集群
func (c *Cluster) Start(port int) error {
	// 启动故障转移管理器
	c.failoverMgr.Start()

	// 启动通信服务
	if err := c.communicator.Start(port); err != nil {
		return err
	}

	// 启动心跳
	go c.communicator.StartHeartbeat()

	// 启动监控
	if c.monitor != nil {
		c.monitor.Start()
	}

	return nil
}

// GetSlots 获取槽分配信息
func (c *Cluster) GetSlots() [CLUSTER_SLOTS]*ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	slots := [CLUSTER_SLOTS]*ClusterNode{}
	copy(slots[:], c.slots[:])
	return slots
}

// String 返回节点信息字符串
func (n *ClusterNode) String() string {
	return fmt.Sprintf("%s %s master - 0-16383", n.NodeID, n.Addr)
}
