package storage

import (
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 数据库系统 (redisDb)
 * ============================================================================
 *
 * Redis 支持多个数据库（默认 16 个），每个数据库独立存储键值对。
 * 数据库结构包含：
 * - keys: 键值对存储（使用哈希表）
 * - expires: 过期时间存储（key -> expire time）
 * - id: 数据库 ID
 *
 * 【键值存储】
 * 使用哈希表（map）存储 key -> RedisObject 的映射。
 * key 是字符串，value 是 RedisObject。
 *
 * 【过期机制】
 * 使用单独的哈希表存储 key -> expire time 的映射。
 * expire time 是 Unix 时间戳（秒）。
 */

// 错误定义在 errors.go 中

// RedisDb Redis 数据库
type RedisDb struct {
	id      int                     // 数据库 ID
	keys    map[string]*RedisObject // 键值对存储
	expires map[string]int64        // 过期时间存储（key -> Unix 时间戳，秒）
	mu      sync.RWMutex            // 读写锁（保证并发安全）
}

// NewRedisDb 创建新的 Redis 数据库
func NewRedisDb(id int) *RedisDb {
	return &RedisDb{
		id:      id,
		keys:    make(map[string]*RedisObject),
		expires: make(map[string]int64),
	}
}

// Set 设置键值对
func (db *RedisDb) Set(key string, obj *RedisObject) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 如果 key 已存在，减少旧对象的引用计数
	if oldObj, exists := db.keys[key]; exists {
		oldObj.DecrRefCount()
	}

	// 设置新对象
	obj.IncrRefCount()
	db.keys[key] = obj
}

// Get 获取键值对
func (db *RedisDb) Get(key string) (*RedisObject, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 检查是否过期
	if db.isExpired(key) {
		return nil, ErrKeyNotFound
	}

	obj, exists := db.keys[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	return obj, nil
}

// Del 删除键值对
func (db *RedisDb) Del(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	obj, exists := db.keys[key]
	if !exists {
		return false
	}

	// 减少引用计数
	obj.DecrRefCount()

	// 删除键值对
	delete(db.keys, key)
	delete(db.expires, key)

	return true
}

// Exists 检查键是否存在
func (db *RedisDb) Exists(key string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isExpired(key) {
		return false
	}

	_, exists := db.keys[key]
	return exists
}

// Type 获取键的类型
func (db *RedisDb) Type(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isExpired(key) {
		return "", ErrKeyNotFound
	}

	obj, exists := db.keys[key]
	if !exists {
		return "", ErrKeyNotFound
	}

	return obj.TypeString(), nil
}

// TTL 获取键的剩余生存时间（秒）
func (db *RedisDb) TTL(key string) (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if !db.Exists(key) {
		return -2, nil // 键不存在
	}

	expire, exists := db.expires[key]
	if !exists {
		return -1, nil // 键存在但没有设置过期时间
	}

	now := time.Now().Unix()
	ttl := expire - now

	if ttl <= 0 {
		// 已过期，删除键
		db.mu.RUnlock()
		db.Del(key)
		db.mu.RLock()
		return -2, nil
	}

	return ttl, nil
}

// Expire 设置键的过期时间（秒）
func (db *RedisDb) Expire(key string, seconds int64) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 检查键是否存在（在锁内检查，避免死锁）
	if db.isExpired(key) {
		return false
	}

	_, exists := db.keys[key]
	if !exists {
		return false
	}

	expire := time.Now().Unix() + seconds
	db.expires[key] = expire

	return true
}

// ExpireAt 设置键的过期时间（Unix 时间戳）
func (db *RedisDb) ExpireAt(key string, timestamp int64) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 检查键是否存在（在锁内检查，避免死锁）
	if db.isExpired(key) {
		return false
	}

	_, exists := db.keys[key]
	if !exists {
		return false
	}

	db.expires[key] = timestamp
	return true
}

// Persist 移除键的过期时间
func (db *RedisDb) Persist(key string) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.expires[key]; !exists {
		return false
	}

	delete(db.expires, key)
	return true
}

// isExpired 检查键是否过期（必须在锁内调用）
func (db *RedisDb) isExpired(key string) bool {
	expire, exists := db.expires[key]
	if !exists {
		return false
	}

	now := time.Now().Unix()
	if now >= expire {
		// 已过期，删除键
		if obj, ok := db.keys[key]; ok {
			obj.DecrRefCount()
		}
		delete(db.keys, key)
		delete(db.expires, key)
		return true
	}

	return false
}

// Keys 获取所有键（支持模式匹配，简化实现：返回所有键）
func (db *RedisDb) Keys(pattern string) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	keys := make([]string, 0, len(db.keys))
	for key := range db.keys {
		if db.isExpired(key) {
			continue
		}
		// 简化实现：如果 pattern 为空或 "*"，返回所有键
		if pattern == "" || pattern == "*" {
			keys = append(keys, key)
		} else {
			// 实际应该实现模式匹配（如 glob pattern）
			keys = append(keys, key)
		}
	}

	return keys
}

// DBSize 获取数据库中的键数量
func (db *RedisDb) DBSize() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	count := 0
	for key := range db.keys {
		if !db.isExpired(key) {
			count++
		}
	}

	return count
}

// FlushDB 清空数据库
func (db *RedisDb) FlushDB() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 减少所有对象的引用计数
	for _, obj := range db.keys {
		obj.DecrRefCount()
	}

	// 清空所有数据
	db.keys = make(map[string]*RedisObject)
	db.expires = make(map[string]int64)
}

// CleanExpiredKeys 清理过期键（应该在后台定期调用）
func (db *RedisDb) CleanExpiredKeys() int {
	db.mu.Lock()
	defer db.mu.Unlock()

	count := 0
	now := time.Now().Unix()

	for key, expire := range db.expires {
		if now >= expire {
			if obj, ok := db.keys[key]; ok {
				obj.DecrRefCount()
			}
			delete(db.keys, key)
			delete(db.expires, key)
			count++
		}
	}

	return count
}

// GetID 获取数据库 ID
func (db *RedisDb) GetID() int {
	return db.id
}
