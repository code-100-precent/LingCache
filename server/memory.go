package server

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/code-100-precent/LingCache/storage"
)

/*
 * ============================================================================
 * 内存优化和统计
 * ============================================================================
 *
 * 实现：
 * - 对象共享（小整数、空字符串）
 * - 内存统计
 */

// SharedObjects 共享对象
type SharedObjects struct {
	integers map[int64]*storage.RedisObject // 共享的小整数
	emptyStr *storage.RedisObject           // 共享的空字符串
	mu       sync.RWMutex
}

// NewSharedObjects 创建共享对象管理器
func NewSharedObjects() *SharedObjects {
	so := &SharedObjects{
		integers: make(map[int64]*storage.RedisObject),
	}

	// 预创建 0-9999 的小整数
	for i := int64(0); i < 10000; i++ {
		so.integers[i] = storage.NewStringObject([]byte(string(rune(i))))
	}

	// 创建共享的空字符串
	so.emptyStr = storage.NewStringObject([]byte(""))

	return so
}

// GetSharedInteger 获取共享的整数对象
func (so *SharedObjects) GetSharedInteger(val int64) *storage.RedisObject {
	if val >= 0 && val < 10000 {
		so.mu.RLock()
		defer so.mu.RUnlock()
		if obj, exists := so.integers[val]; exists {
			obj.IncrRefCount()
			return obj
		}
	}
	return nil
}

// GetSharedEmptyString 获取共享的空字符串对象
func (so *SharedObjects) GetSharedEmptyString() *storage.RedisObject {
	so.mu.RLock()
	defer so.mu.RUnlock()
	so.emptyStr.IncrRefCount()
	return so.emptyStr
}

// MemoryStats 内存统计
type MemoryStats struct {
	usedMemory      int64
	usedMemoryPeak  int64
	usedMemoryHuman string
	mu              sync.RWMutex
}

// NewMemoryStats 创建内存统计
func NewMemoryStats() *MemoryStats {
	return &MemoryStats{}
}

// Update 更新内存统计
func (ms *MemoryStats) Update() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	ms.usedMemory = int64(m.Alloc)

	if ms.usedMemory > ms.usedMemoryPeak {
		ms.usedMemoryPeak = ms.usedMemory
	}

	// 格式化内存大小
	ms.usedMemoryHuman = formatBytes(ms.usedMemory)
}

// GetUsedMemory 获取已使用内存
func (ms *MemoryStats) GetUsedMemory() int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.usedMemory
}

// GetUsedMemoryPeak 获取内存峰值
func (ms *MemoryStats) GetUsedMemoryPeak() int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.usedMemoryPeak
}

// GetUsedMemoryHuman 获取格式化的内存大小
func (ms *MemoryStats) GetUsedMemoryHuman() string {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.usedMemoryHuman
}

// formatBytes 格式化字节数
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
