package server

import (
	"sync"
	"time"

	"github.com/code-100-precent/LingCache/protocol"
)

/*
 * ============================================================================
 * 阻塞命令实现
 * ============================================================================
 *
 * 实现真正的阻塞机制：
 * - 客户端等待队列
 * - 超时机制
 * - 唤醒机制（当有数据时）
 */

// BlockingClient 阻塞的客户端
type BlockingClient struct {
	client     *Client
	keys       []string
	timeout    time.Duration
	notify     chan *protocol.RESPValue
	expireTime time.Time
}

// BlockingManager 阻塞管理器
type BlockingManager struct {
	waitingClients map[string][]*BlockingClient // key -> clients
	mu             sync.RWMutex
}

// NewBlockingManager 创建阻塞管理器
func NewBlockingManager() *BlockingManager {
	return &BlockingManager{
		waitingClients: make(map[string][]*BlockingClient),
	}
}

// Wait 等待键有数据
func (bm *BlockingManager) Wait(client *Client, keys []string, timeout int64) *BlockingClient {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bc := &BlockingClient{
		client:     client,
		keys:       keys,
		timeout:    time.Duration(timeout) * time.Second,
		notify:     make(chan *protocol.RESPValue, 1),
		expireTime: time.Now().Add(time.Duration(timeout) * time.Second),
	}

	// 将客户端添加到每个键的等待列表
	for _, key := range keys {
		if bm.waitingClients[key] == nil {
			bm.waitingClients[key] = make([]*BlockingClient, 0)
		}
		bm.waitingClients[key] = append(bm.waitingClients[key], bc)
	}

	return bc
}

// Notify 通知等待的客户端
func (bm *BlockingManager) Notify(key string, value *protocol.RESPValue) bool {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	clients, exists := bm.waitingClients[key]
	if !exists || len(clients) == 0 {
		return false
	}

	// 找到第一个未过期的客户端
	for i, bc := range clients {
		if time.Now().Before(bc.expireTime) {
			// 通知这个客户端
			select {
			case bc.notify <- value:
			default:
			}

			// 从所有键的等待列表中移除
			bm.removeClient(bc)

			// 从当前列表移除
			bm.waitingClients[key] = append(clients[:i], clients[i+1:]...)
			if len(bm.waitingClients[key]) == 0 {
				delete(bm.waitingClients, key)
			}

			return true
		}
	}

	// 清理过期的客户端
	bm.cleanExpired(key)

	return false
}

// removeClient 从所有键的等待列表中移除客户端
func (bm *BlockingManager) removeClient(bc *BlockingClient) {
	for _, key := range bc.keys {
		clients := bm.waitingClients[key]
		for i, c := range clients {
			if c == bc {
				bm.waitingClients[key] = append(clients[:i], clients[i+1:]...)
				if len(bm.waitingClients[key]) == 0 {
					delete(bm.waitingClients, key)
				}
				break
			}
		}
	}
}

// cleanExpired 清理过期的客户端
func (bm *BlockingManager) cleanExpired(key string) {
	clients := bm.waitingClients[key]
	now := time.Now()
	valid := make([]*BlockingClient, 0)

	for _, bc := range clients {
		if now.Before(bc.expireTime) {
			valid = append(valid, bc)
		}
	}

	if len(valid) == 0 {
		delete(bm.waitingClients, key)
	} else {
		bm.waitingClients[key] = valid
	}
}

// CleanExpired 清理所有过期的客户端
func (bm *BlockingManager) CleanExpired() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for key := range bm.waitingClients {
		bm.cleanExpired(key)
	}
}
