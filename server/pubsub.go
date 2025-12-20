package server

import (
	"sync"

	"github.com/code-100-precent/LingCache/protocol"
)

/*
 * ============================================================================
 * Redis 发布订阅实现
 * ============================================================================
 *
 * Redis 发布订阅（Pub/Sub）是一种消息传递模式：
 * - PUBLISH: 发布消息到频道
 * - SUBSCRIBE: 订阅频道
 * - UNSUBSCRIBE: 取消订阅
 * - PSUBSCRIBE: 模式订阅
 * - PUNSUBSCRIBE: 取消模式订阅
 * - PUBSUB: 查看订阅信息
 */

// PubSubManager 发布订阅管理器
type PubSubManager struct {
	channels map[string]map[*Client]bool // 频道 -> 客户端集合
	patterns map[string]map[*Client]bool // 模式 -> 客户端集合
	mu       sync.RWMutex
}

// NewPubSubManager 创建发布订阅管理器
func NewPubSubManager() *PubSubManager {
	return &PubSubManager{
		channels: make(map[string]map[*Client]bool),
		patterns: make(map[string]map[*Client]bool),
	}
}

// Subscribe 订阅频道
func (ps *PubSubManager) Subscribe(client *Client, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.channels[channel] == nil {
		ps.channels[channel] = make(map[*Client]bool)
	}
	ps.channels[channel][client] = true
}

// Unsubscribe 取消订阅频道
func (ps *PubSubManager) Unsubscribe(client *Client, channel string) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if clients, exists := ps.channels[channel]; exists {
		delete(clients, client)
		if len(clients) == 0 {
			delete(ps.channels, channel)
		}
		return 1
	}
	return 0
}

// PSubscribe 模式订阅
func (ps *PubSubManager) PSubscribe(client *Client, pattern string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.patterns[pattern] == nil {
		ps.patterns[pattern] = make(map[*Client]bool)
	}
	ps.patterns[pattern][client] = true
}

// PUnsubscribe 取消模式订阅
func (ps *PubSubManager) PUnsubscribe(client *Client, pattern string) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if clients, exists := ps.patterns[pattern]; exists {
		delete(clients, client)
		if len(clients) == 0 {
			delete(ps.patterns, pattern)
		}
		return 1
	}
	return 0
}

// Publish 发布消息
func (ps *PubSubManager) Publish(channel string, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	count := 0

	// 发送给频道订阅者
	if clients, exists := ps.channels[channel]; exists {
		for client := range clients {
			// 发送消息：*3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$5\r\nhello\r\n
			resp := protocol.NewArray([]*protocol.RESPValue{
				protocol.NewBulkString("message"),
				protocol.NewBulkString(channel),
				protocol.NewBulkString(message),
			})
			client.writeResponse(resp)
			count++
		}
	}

	// 发送给模式订阅者（简化实现：不匹配模式）

	return count
}

// NumSub 获取频道的订阅者数量
func (ps *PubSubManager) NumSub(channel string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if clients, exists := ps.channels[channel]; exists {
		return len(clients)
	}
	return 0
}

// NumPat 获取模式订阅数量
func (ps *PubSubManager) NumPat() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return len(ps.patterns)
}

// Channels 获取所有频道
func (ps *PubSubManager) Channels() []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	channels := make([]string, 0, len(ps.channels))
	for channel := range ps.channels {
		channels = append(channels, channel)
	}
	return channels
}
