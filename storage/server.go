package storage

import (
	"sync"
)

/*
 * ============================================================================
 * Redis 服务器系统
 * ============================================================================
 *
 * Redis 服务器管理多个数据库，并提供统一的访问接口。
 * 服务器结构包含：
 * - dbs: 数据库数组（默认 16 个）
 * - dbnum: 数据库数量
 * - currentDb: 当前选中的数据库（默认 0）
 *
 * 【数据库选择】
 * 客户端可以使用 SELECT 命令选择不同的数据库。
 * 每个数据库独立存储键值对，互不干扰。
 */

// RedisServer Redis 服务器
type RedisServer struct {
	dbs       []*RedisDb // 数据库数组
	dbnum     int        // 数据库数量
	currentDb int        // 当前选中的数据库
	mu        sync.RWMutex
}

// NewRedisServer 创建新的 Redis 服务器
func NewRedisServer(dbnum int) *RedisServer {
	if dbnum <= 0 {
		dbnum = 16 // 默认 16 个数据库
	}

	server := &RedisServer{
		dbs:       make([]*RedisDb, dbnum),
		dbnum:     dbnum,
		currentDb: 0,
	}

	// 初始化所有数据库
	for i := 0; i < dbnum; i++ {
		server.dbs[i] = NewRedisDb(i)
	}

	return server
}

// SelectDb 选择数据库
func (s *RedisServer) SelectDb(dbIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if dbIndex < 0 || dbIndex >= s.dbnum {
		return ErrInvalidDbIndex
	}

	s.currentDb = dbIndex
	return nil
}

// GetCurrentDb 获取当前数据库
func (s *RedisServer) GetCurrentDb() *RedisDb {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.dbs[s.currentDb]
}

// GetDb 获取指定数据库
func (s *RedisServer) GetDb(dbIndex int) (*RedisDb, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if dbIndex < 0 || dbIndex >= s.dbnum {
		return nil, ErrInvalidDbIndex
	}

	return s.dbs[dbIndex], nil
}

// FlushAll 清空所有数据库
func (s *RedisServer) FlushAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, db := range s.dbs {
		db.FlushDB()
	}
}

// GetDbNum 获取数据库数量
func (s *RedisServer) GetDbNum() int {
	return s.dbnum
}

// ServerError 服务器错误
type ServerError struct {
	Message string
}

func (e *ServerError) Error() string {
	return e.Message
}
