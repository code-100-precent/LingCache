package cluster

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 集群配置持久化实现 - Cluster Configuration Persistence
 * ============================================================================
 *
 * 【核心原理】
 * Redis Cluster 需要持久化集群配置，以便节点重启后能够恢复集群状态。
 * 配置信息包括：节点信息、槽分配、节点状态等。
 *
 * 1. 配置文件格式
 *    - 文件名：nodes.conf（默认）
 *    - 格式：每行一个节点信息
 *    - 内容：节点 ID、地址、角色、槽分配、主节点 ID 等
 *
 * 2. 配置内容
 *    - 节点 ID：每个节点的唯一标识
 *    - 节点地址：IP:端口
 *    - 节点角色：master 或 slave
 *    - 槽分配：节点负责的槽范围
 *    - 主从关系：从节点的主节点 ID
 *    - 节点状态：connected、disconnected 等
 *
 * 3. 持久化时机
 *    - 节点加入集群时
 *    - 槽分配变化时
 *    - 故障转移后
 *    - 定期保存（防止数据丢失）
 *
 * 4. 配置加载
 *    - 节点启动时加载配置
 *    - 验证配置有效性
 *    - 与集群其他节点同步
 *
 * 【面试题】
 * Q1: 为什么 Redis Cluster 需要持久化配置？
 * A1: 持久化的必要性：
 *     1. **节点重启恢复**：节点重启后需要知道集群拓扑
 *     2. **槽分配信息**：需要知道哪些槽由哪些节点负责
 *     3. **节点关系**：需要知道主从关系、节点状态
 *     4. **快速启动**：避免重新发现集群拓扑
 *     5. **数据一致性**：确保节点重启后集群状态一致
 *     如果不持久化：
 *     - 节点重启后需要重新加入集群
 *     - 需要重新分配槽
 *     - 可能导致数据不一致
 *
 * Q2: Redis Cluster 的节点 ID 是如何生成的？
 * A2: 节点 ID 生成规则：
 *     - 格式：40 个十六进制字符（160 位）
 *     - 生成方式：使用 SHA1 哈希算法
 *     - 输入：节点地址 + 随机数 + 时间戳
 *     - 特点：全局唯一、不可预测
 *     节点 ID 的作用：
 *     - 唯一标识节点
 *     - 用于 Gossip 协议
 *     - 用于故障转移投票
 *     持久化节点 ID：
 *     - 节点 ID 保存在配置文件中
 *     - 重启后使用相同的节点 ID
 *     - 如果配置文件丢失，生成新的节点 ID
 *
 * Q3: 集群配置如何保证一致性？
 * A3: 一致性保证机制：
 *     1. **本地持久化**：每个节点保存自己的配置
 *     2. **Gossip 同步**：通过 Gossip 协议同步配置
 *     3. **版本号**：使用配置版本号检测冲突
 *     4. **多数派原则**：配置变更需要多数节点同意
 *     可能的问题：
 *     - 配置文件损坏：需要从其他节点恢复
 *     - 配置不一致：通过 Gossip 协议同步
 *     - 配置冲突：使用版本号解决
 *
 * Q4: 节点重启后如何恢复集群状态？
 * A4: 恢复流程：
 *     1. **加载配置**：从 nodes.conf 加载配置
 *     2. **验证配置**：检查节点 ID、槽分配等
 *     3. **连接集群**：尝试连接配置中的其他节点
 *     4. **同步状态**：通过 Gossip 协议同步最新状态
 *     5. **验证槽分配**：确认槽分配是否正确
 *     6. **恢复服务**：如果验证通过，恢复服务
 *     如果配置丢失：
 *     - 生成新的节点 ID
 *     - 需要重新加入集群
 *     - 需要重新分配槽
 *
 * Q5: 集群配置文件的格式是什么？
 * A5: 配置文件格式（nodes.conf）：
 *     每行一个节点信息，格式：
 *     <node-id> <ip>:<port>@<cport> <flags> <master-id> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots>
 *     示例：
 *     a1b2c3d4e5f6... 127.0.0.1:7000@17000 master - 0 1234567890 1234567891 0 connected 0-5460
 *     字段说明：
 *     - node-id: 节点 ID（40 字符）
 *     - ip:port@cport: 客户端端口和集群端口
 *     - flags: 节点标志（master/slave/myself等）
 *     - master-id: 主节点 ID（从节点才有）
 *     - ping-sent: 最后发送 PING 的时间
 *     - pong-recv: 最后接收 PONG 的时间
 *     - config-epoch: 配置版本号
 *     - link-state: 连接状态（connected/disconnected）
 *     - slots: 槽分配（如 0-5460）
 *
 * Q6: 如何备份和恢复集群配置？
 * A6: 备份策略：
 *     1. **定期备份**：定期复制 nodes.conf 文件
 *     2. **多节点备份**：每个节点都有配置副本
 *     3. **版本控制**：使用版本号管理配置变更
 *     恢复策略：
 *     1. **从备份恢复**：复制备份文件到节点
 *     2. **从其他节点恢复**：通过 CLUSTER NODES 命令获取配置
 *     3. **手动重建**：如果所有配置丢失，手动重建集群
 *     注意事项：
 *     - 确保配置文件的权限和所有权
 *     - 定期验证配置的有效性
 *     - 保留配置变更历史
 */

// ClusterConfig 集群配置
type ClusterConfig struct {
	Myself   *NodeConfig            `json:"myself"`
	Nodes    map[string]*NodeConfig `json:"nodes"`     // nodeID -> config
	Slots    map[int]string         `json:"slots"`     // slot -> nodeID
	Version  int64                  `json:"version"`   // 配置版本号
	LastSave int64                  `json:"last_save"` // 最后保存时间
}

// NodeConfig 节点配置
type NodeConfig struct {
	NodeID      string   `json:"node_id"`
	Addr        string   `json:"addr"`
	Role        string   `json:"role"`         // master/slave
	MasterID    string   `json:"master_id"`    // 从节点的主节点 ID
	Slots       []int    `json:"slots"`        // 负责的槽
	Flags       []string `json:"flags"`        // 节点标志
	ConfigEpoch int64    `json:"config_epoch"` // 配置版本号
}

// ConfigPersistence 配置持久化管理器
type ConfigPersistence struct {
	cluster    *Cluster
	configPath string
	config     *ClusterConfig
	mu         sync.RWMutex
}

// NewConfigPersistence 创建配置持久化管理器
func NewConfigPersistence(cluster *Cluster, configPath string) *ConfigPersistence {
	if configPath == "" {
		configPath = "nodes.conf"
	}

	return &ConfigPersistence{
		cluster:    cluster,
		configPath: configPath,
		config: &ClusterConfig{
			Nodes: make(map[string]*NodeConfig),
			Slots: make(map[int]string),
		},
	}
}

// SaveConfig 保存配置到文件
func (cp *ConfigPersistence) SaveConfig() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// 构建配置
	cp.buildConfig()

	// 序列化配置
	data, err := json.MarshalIndent(cp.config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %v", err)
	}

	// 写入文件（原子写入）
	tmpPath := cp.configPath + ".tmp"
	if err := ioutil.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config: %v", err)
	}

	// 原子替换
	if err := os.Rename(tmpPath, cp.configPath); err != nil {
		return fmt.Errorf("failed to rename config: %v", err)
	}

	cp.config.LastSave = getCurrentTimestamp()

	return nil
}

// LoadConfig 从文件加载配置
func (cp *ConfigPersistence) LoadConfig() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// 检查文件是否存在
	if _, err := os.Stat(cp.configPath); os.IsNotExist(err) {
		// 配置文件不存在，创建新配置
		cp.config = &ClusterConfig{
			Nodes: make(map[string]*NodeConfig),
			Slots: make(map[int]string),
		}
		return nil
	}

	// 读取文件
	data, err := ioutil.ReadFile(cp.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config: %v", err)
	}

	// 反序列化
	config := &ClusterConfig{}
	if err := json.Unmarshal(data, config); err != nil {
		return fmt.Errorf("failed to unmarshal config: %v", err)
	}

	cp.config = config

	// 恢复集群状态
	cp.restoreClusterState()

	return nil
}

// buildConfig 构建配置
func (cp *ConfigPersistence) buildConfig() {
	cp.config.Nodes = make(map[string]*NodeConfig)
	cp.config.Slots = make(map[int]string)

	// 保存当前节点
	myself := cp.cluster.GetMyself()
	cp.config.Myself = &NodeConfig{
		NodeID: myself.NodeID,
		Addr:   myself.Addr,
		Role:   "master",
		Slots:  myself.Slots,
		Flags:  []string{"myself", "master"},
	}
	cp.config.Nodes[myself.NodeID] = cp.config.Myself

	// 保存其他节点
	for nodeID, node := range cp.cluster.nodes {
		if nodeID == myself.NodeID {
			continue
		}

		nodeConfig := &NodeConfig{
			NodeID: node.NodeID,
			Addr:   node.Addr,
			Role:   "master",
			Slots:  node.Slots,
			Flags:  []string{"master"},
		}

		if node.Master != nil {
			nodeConfig.Role = "slave"
			nodeConfig.MasterID = node.Master.NodeID
			nodeConfig.Flags = []string{"slave"}
		}

		cp.config.Nodes[nodeID] = nodeConfig
	}

	// 保存槽分配
	slots := cp.cluster.GetSlots()
	for slot, node := range slots {
		if node != nil {
			cp.config.Slots[slot] = node.NodeID
		}
	}

	cp.config.Version++
}

// restoreClusterState 恢复集群状态
func (cp *ConfigPersistence) restoreClusterState() {
	if cp.config == nil {
		return
	}

	// 恢复节点
	for nodeID, nodeConfig := range cp.config.Nodes {
		cp.cluster.AddNode(nodeID, nodeConfig.Addr)
	}

	// 恢复槽分配
	for slot, nodeID := range cp.config.Slots {
		cp.cluster.AssignSlots(nodeID, []int{slot})
	}

	// 恢复主从关系
	for nodeID, nodeConfig := range cp.config.Nodes {
		if nodeConfig.Role == "slave" && nodeConfig.MasterID != "" {
			node := cp.cluster.nodes[nodeID]
			master := cp.cluster.nodes[nodeConfig.MasterID]
			if node != nil && master != nil {
				node.Master = master
				master.Replicas = append(master.Replicas, node)
			}
		}
	}
}

// SaveNodesConf 保存为 Redis 格式的 nodes.conf
func (cp *ConfigPersistence) SaveNodesConf() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	cp.buildConfig()

	// 构建 Redis 格式的配置
	var lines []string

	// 保存每个节点
	for nodeID, nodeConfig := range cp.config.Nodes {
		line := cp.formatNodeLine(nodeID, nodeConfig)
		lines = append(lines, line)
	}

	// 写入文件
	content := ""
	for _, line := range lines {
		content += line + "\n"
	}

	tmpPath := cp.configPath + ".tmp"
	if err := ioutil.WriteFile(tmpPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write nodes.conf: %v", err)
	}

	if err := os.Rename(tmpPath, cp.configPath); err != nil {
		return fmt.Errorf("failed to rename nodes.conf: %v", err)
	}

	return nil
}

// formatNodeLine 格式化节点行（Redis 格式）
func (cp *ConfigPersistence) formatNodeLine(nodeID string, config *NodeConfig) string {
	// 格式：<node-id> <ip>:<port>@<cport> <flags> <master-id> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots>
	flags := ""
	if len(config.Flags) > 0 {
		flags = config.Flags[0]
		for i := 1; i < len(config.Flags); i++ {
			flags += "," + config.Flags[i]
		}
	}

	masterID := "-"
	if config.MasterID != "" {
		masterID = config.MasterID
	}

	slots := ""
	if len(config.Slots) > 0 {
		// 简化实现：只显示槽范围
		slots = fmt.Sprintf("%d-%d", config.Slots[0], config.Slots[len(config.Slots)-1])
	}

	// 解析地址
	host := "127.0.0.1"
	port := "6379"
	if config.Addr != "" {
		// 简化解析
		host = config.Addr
	}

	return fmt.Sprintf("%s %s:%s@%s %s %s 0 0 %d connected %s",
		nodeID, host, port, port, flags, masterID, config.ConfigEpoch, slots)
}

// LoadNodesConf 从 Redis 格式的 nodes.conf 加载
func (cp *ConfigPersistence) LoadNodesConf() error {
	// 简化实现：使用 JSON 格式
	return cp.LoadConfig()
}

// GetConfigPath 获取配置文件路径
func (cp *ConfigPersistence) GetConfigPath() string {
	return cp.configPath
}

// SetConfigPath 设置配置文件路径
func (cp *ConfigPersistence) SetConfigPath(path string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.configPath = path
}

// GetConfig 获取配置
func (cp *ConfigPersistence) GetConfig() *ClusterConfig {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return cp.config
}

// getCurrentTimestamp 获取当前时间戳
func getCurrentTimestamp() int64 {
	return time.Now().Unix()
}

// EnsureConfigDir 确保配置目录存在
func (cp *ConfigPersistence) EnsureConfigDir() error {
	dir := filepath.Dir(cp.configPath)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0755)
}
