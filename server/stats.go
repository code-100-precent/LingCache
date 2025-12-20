package server

import (
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 统计和监控实现
 * ============================================================================
 *
 * 统计信息包括：
 * - 命令执行次数
 * - 命令执行时间
 * - 内存使用
 * - 连接数
 * - 键空间统计
 */

// Stats 统计信息
type Stats struct {
	TotalCommandsProcessed   int64
	TotalConnectionsReceived int64
	KeyspaceHits             int64
	KeyspaceMisses           int64
	SlowLog                  []*SlowLogEntry
	CommandStats             map[string]*CommandStat
	mu                       sync.RWMutex
}

// SlowLogEntry 慢查询日志条目
type SlowLogEntry struct {
	ID        int64
	Timestamp time.Time
	Duration  time.Duration
	Command   string
	Args      []string
}

// CommandStat 命令统计
type CommandStat struct {
	Calls     int64
	TotalTime time.Duration
	MaxTime   time.Duration
}

// NewStats 创建统计信息
func NewStats() *Stats {
	return &Stats{
		SlowLog:      make([]*SlowLogEntry, 0),
		CommandStats: make(map[string]*CommandStat),
	}
}

// RecordCommand 记录命令执行
func (s *Stats) RecordCommand(cmdName string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TotalCommandsProcessed++

	stat, exists := s.CommandStats[cmdName]
	if !exists {
		stat = &CommandStat{}
		s.CommandStats[cmdName] = stat
	}

	stat.Calls++
	stat.TotalTime += duration
	if duration > stat.MaxTime {
		stat.MaxTime = duration
	}

	// 记录慢查询（超过 10ms）
	if duration > 10*time.Millisecond {
		entry := &SlowLogEntry{
			ID:        s.TotalCommandsProcessed,
			Timestamp: time.Now(),
			Duration:  duration,
			Command:   cmdName,
		}
		s.SlowLog = append(s.SlowLog, entry)

		// 只保留最近 128 条
		if len(s.SlowLog) > 128 {
			s.SlowLog = s.SlowLog[1:]
		}
	}
}

// RecordConnection 记录连接
func (s *Stats) RecordConnection() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalConnectionsReceived++
}

// RecordKeyspaceHit 记录键空间命中
func (s *Stats) RecordKeyspaceHit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.KeyspaceHits++
}

// RecordKeyspaceMiss 记录键空间未命中
func (s *Stats) RecordKeyspaceMiss() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.KeyspaceMisses++
}

// GetSlowLog 获取慢查询日志
func (s *Stats) GetSlowLog(count int) []*SlowLogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if count <= 0 || count > len(s.SlowLog) {
		count = len(s.SlowLog)
	}

	start := len(s.SlowLog) - count
	if start < 0 {
		start = 0
	}

	result := make([]*SlowLogEntry, count)
	copy(result, s.SlowLog[start:])
	return result
}
