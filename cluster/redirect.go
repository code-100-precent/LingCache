package cluster

import (
	"fmt"
	"sync"
)

/*
 * ============================================================================
 * Redis 集群客户端重定向实现 - MOVED/ASK Redirect
 * ============================================================================
 *
 * 【核心原理】
 * Redis Cluster 使用重定向机制处理槽迁移和节点变化。
 * 客户端需要能够正确处理 MOVED 和 ASK 重定向，并更新本地路由缓存。
 *
 * 1. MOVED 重定向
 *    - 格式：MOVED <slot> <host>:<port>
 *    - 含义：槽已永久迁移到新节点
 *    - 处理：更新客户端路由缓存，重新发送请求到新节点
 *    - 场景：槽迁移完成、节点故障转移后
 *
 * 2. ASK 重定向
 *    - 格式：ASK <slot> <host>:<port>
 *    - 含义：槽正在迁移，临时重定向
 *    - 处理：发送 ASKING 命令，然后发送原请求，不更新缓存
 *    - 场景：槽迁移过程中
 *
 * 3. 客户端路由缓存
 *    - 维护槽到节点的映射：slot -> (host, port)
 *    - 收到 MOVED 时更新缓存
 *    - 收到 ASK 时不更新缓存（临时重定向）
 *    - 缓存失效：节点故障、槽迁移
 *
 * 4. ASKING 命令
 *    - 用于处理 ASK 重定向
 *    - 格式：ASKING
 *    - 作用：告诉目标节点，这是迁移过程中的请求
 *    - 目标节点会处理该请求，即使槽还未完全迁移
 *
 * 【面试题】
 * Q1: MOVED 和 ASK 重定向有什么区别？
 * A1: 主要区别：
 *     - **MOVED**：永久重定向
 *       * 槽已完全迁移到新节点
 *       * 客户端应该更新路由缓存
 *       * 后续请求直接发送到新节点
 *     - **ASK**：临时重定向
 *       * 槽正在迁移过程中
 *       * 客户端不应该更新路由缓存
 *       * 需要先发送 ASKING 命令
 *       * 只针对当前请求有效
 *     使用场景：
 *     - MOVED：槽迁移完成、故障转移后
 *     - ASK：槽迁移过程中
 *
 * Q2: 为什么 ASK 重定向不更新客户端缓存？
 * A2: 原因：
 *     1. **临时性**：槽正在迁移，状态不稳定
 *     2. **避免缓存污染**：如果更新缓存，后续请求可能发送到错误节点
 *     3. **迁移完成**：迁移完成后会收到 MOVED，再更新缓存
 *     4. **性能考虑**：迁移过程通常很快，不需要频繁更新缓存
 *     例如：
 *     - 槽 1000 正在从 Node1 迁移到 Node2
 *     - 客户端请求键 "key1"（槽 1000）
 *     - Node1 返回 ASK Node2
 *     - 客户端发送 ASKING + 请求到 Node2
 *     - 但不更新缓存，下次请求仍发送到 Node1
 *
 * Q3: 客户端如何处理重定向？
 * A3: 处理流程：
 *     1. **发送请求**：根据本地缓存发送到节点
 *     2. **接收响应**：
 *        * 正常响应：处理结果
 *        * MOVED <slot> <node>：更新缓存，重试到新节点
 *        * ASK <slot> <node>：发送 ASKING + 请求到新节点，不更新缓存
 *     3. **重试机制**：
 *        * 限制重试次数（避免无限循环）
 *        * 记录重定向链（避免循环重定向）
 *     4. **错误处理**：
 *        * 网络错误：尝试其他节点
 *        * 节点故障：更新缓存，尝试其他节点
 *
 * Q4: 客户端路由缓存如何更新？
 * A4: 更新策略：
 *     1. **MOVED 重定向**：立即更新缓存
 *     2. **节点故障**：清除该节点的所有槽缓存
 *     3. **集群拓扑变化**：通过 CLUSTER SLOTS 命令更新
 *     4. **定期刷新**：定期获取集群拓扑，更新缓存
 *     缓存结构：
 *     - slot -> (host, port, nodeID)
 *     - 支持批量更新
 *     优化：
 *     - 使用版本号避免频繁更新
 *     - 增量更新（只更新变化的槽）
 *
 * Q5: ASKING 命令的作用是什么？
 * A5: ASKING 命令的作用：
 *     1. **标识迁移请求**：告诉目标节点这是迁移过程中的请求
 *     2. **允许临时访问**：即使槽还未完全迁移，也允许处理请求
 *     3. **避免 MOVED**：防止目标节点返回 MOVED 重定向
 *     使用场景：
 *     - 槽正在从 Node1 迁移到 Node2
 *     - 客户端请求键 "key1"，收到 ASK Node2
 *     - 客户端发送：ASKING + GET key1 到 Node2
 *     - Node2 检查：如果键存在，返回结果；如果不存在，返回 NIL
 *
 * Q6: 重定向可能导致的问题有哪些？
 * A6: 可能的问题：
 *     1. **重定向循环**：
 *        * 原因：节点间槽分配不一致
 *        * 解决：限制重定向次数，记录重定向链
 *     2. **性能影响**：
 *        * 原因：频繁重定向增加延迟
 *        * 解决：及时更新缓存，使用 CLUSTER SLOTS 批量更新
 *     3. **数据不一致**：
 *        * 原因：迁移过程中，键可能在不同节点
 *        * 解决：使用 ASKING 命令，确保请求发送到正确节点
 *     4. **缓存失效**：
 *        * 原因：节点故障、槽迁移
 *        * 解决：实现缓存失效机制，及时更新
 *
 * Q7: 如何优化客户端重定向性能？
 * A7: 优化策略：
 *     1. **批量更新缓存**：使用 CLUSTER SLOTS 一次性更新所有槽
 *     2. **智能重试**：记录重定向历史，避免重复重定向
 *     3. **连接池**：维护到各节点的连接池，减少连接开销
 *     4. **本地缓存**：缓存槽到节点的映射，减少重定向
 *     5. **预取拓扑**：定期获取集群拓扑，提前更新缓存
 *     6. **并行请求**：对于批量操作，并行发送到不同节点
 */

// RedirectType 重定向类型
type RedirectType int

const (
	REDIRECT_NONE RedirectType = iota
	REDIRECT_MOVED
	REDIRECT_ASK
)

// RedirectInfo 重定向信息
type RedirectInfo struct {
	Type     RedirectType
	Slot     int
	Host     string
	Port     int
	NodeID   string
	IsAsking bool // 是否已发送 ASKING
}

// ClientRedirectCache 客户端重定向缓存
type ClientRedirectCache struct {
	slotToNode  map[int]*NodeInfo // slot -> node info
	nodeToSlots map[string][]int  // nodeID -> slots
	mu          sync.RWMutex
}

// NodeInfo 节点信息
type NodeInfo struct {
	NodeID string
	Host   string
	Port   int
}

// NewClientRedirectCache 创建客户端重定向缓存
func NewClientRedirectCache() *ClientRedirectCache {
	return &ClientRedirectCache{
		slotToNode:  make(map[int]*NodeInfo),
		nodeToSlots: make(map[string][]int),
	}
}

// GetNodeForSlot 获取槽对应的节点
func (crc *ClientRedirectCache) GetNodeForSlot(slot int) (*NodeInfo, bool) {
	crc.mu.RLock()
	defer crc.mu.RUnlock()

	node, exists := crc.slotToNode[slot]
	return node, exists
}

// UpdateSlotNode 更新槽到节点的映射（MOVED 重定向）
func (crc *ClientRedirectCache) UpdateSlotNode(slot int, nodeID, host string, port int) {
	crc.mu.Lock()
	defer crc.mu.Unlock()

	// 移除旧节点的槽
	if oldNode, exists := crc.slotToNode[slot]; exists {
		oldSlots := crc.nodeToSlots[oldNode.NodeID]
		newSlots := make([]int, 0, len(oldSlots))
		for _, s := range oldSlots {
			if s != slot {
				newSlots = append(newSlots, s)
			}
		}
		crc.nodeToSlots[oldNode.NodeID] = newSlots
	}

	// 更新槽到节点映射
	nodeInfo := &NodeInfo{
		NodeID: nodeID,
		Host:   host,
		Port:   port,
	}
	crc.slotToNode[slot] = nodeInfo

	// 更新节点到槽映射
	crc.nodeToSlots[nodeID] = append(crc.nodeToSlots[nodeID], slot)
}

// RemoveNode 移除节点（节点故障）
func (crc *ClientRedirectCache) RemoveNode(nodeID string) {
	crc.mu.Lock()
	defer crc.mu.Unlock()

	// 获取节点的所有槽
	slots, exists := crc.nodeToSlots[nodeID]
	if !exists {
		return
	}

	// 移除槽映射
	for _, slot := range slots {
		delete(crc.slotToNode, slot)
	}

	// 移除节点映射
	delete(crc.nodeToSlots, nodeID)
}

// UpdateFromClusterSlots 从 CLUSTER SLOTS 更新缓存
func (crc *ClientRedirectCache) UpdateFromClusterSlots(slots [][]interface{}) {
	crc.mu.Lock()
	defer crc.mu.Unlock()

	// 清空旧缓存
	crc.slotToNode = make(map[int]*NodeInfo)
	crc.nodeToSlots = make(map[string][]int)

	// 更新缓存
	for _, slotInfo := range slots {
		if len(slotInfo) < 3 {
			continue
		}

		startSlot := int(slotInfo[0].(int64))
		endSlot := int(slotInfo[1].(int64))
		nodeInfo := slotInfo[2].([]interface{})

		if len(nodeInfo) < 2 {
			continue
		}

		host := nodeInfo[0].(string)
		port := int(nodeInfo[1].(int64))
		nodeID := ""
		if len(nodeInfo) > 2 {
			nodeID = nodeInfo[2].(string)
		}

		// 更新槽范围
		for slot := startSlot; slot <= endSlot; slot++ {
			node := &NodeInfo{
				NodeID: nodeID,
				Host:   host,
				Port:   port,
			}
			crc.slotToNode[slot] = node
			crc.nodeToSlots[nodeID] = append(crc.nodeToSlots[nodeID], slot)
		}
	}
}

// ParseMOVEDRedirect 解析 MOVED 重定向
func ParseMOVEDRedirect(response string) (*RedirectInfo, error) {
	// MOVED <slot> <host>:<port>
	var slot int
	var host string
	var port int

	_, err := fmt.Sscanf(response, "MOVED %d %s:%d", &slot, &host, &port)
	if err != nil {
		return nil, err
	}

	return &RedirectInfo{
		Type: REDIRECT_MOVED,
		Slot: slot,
		Host: host,
		Port: port,
	}, nil
}

// ParseASKRedirect 解析 ASK 重定向
func ParseASKRedirect(response string) (*RedirectInfo, error) {
	// ASK <slot> <host>:<port>
	var slot int
	var host string
	var port int

	_, err := fmt.Sscanf(response, "ASK %d %s:%d", &slot, &host, &port)
	if err != nil {
		return nil, err
	}

	return &RedirectInfo{
		Type:     REDIRECT_ASK,
		Slot:     slot,
		Host:     host,
		Port:     port,
		IsAsking: false,
	}, nil
}

// HandleRedirect 处理重定向
func (crc *ClientRedirectCache) HandleRedirect(redirect *RedirectInfo) {
	switch redirect.Type {
	case REDIRECT_MOVED:
		// 更新缓存
		crc.UpdateSlotNode(redirect.Slot, redirect.NodeID, redirect.Host, redirect.Port)
	case REDIRECT_ASK:
		// 不更新缓存，只处理当前请求
		// 需要先发送 ASKING 命令
		redirect.IsAsking = true
	}
}

// GetRedirectAddress 获取重定向地址
func (ri *RedirectInfo) GetRedirectAddress() string {
	return fmt.Sprintf("%s:%d", ri.Host, ri.Port)
}

// ShouldUpdateCache 是否应该更新缓存
func (ri *RedirectInfo) ShouldUpdateCache() bool {
	return ri.Type == REDIRECT_MOVED
}
