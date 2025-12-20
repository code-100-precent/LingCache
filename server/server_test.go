package server

import (
	"testing"
)

// TestServerCreation 测试服务器创建
func TestServerCreation(t *testing.T) {
	server := NewServer(":6379", 16)

	if server == nil {
		t.Fatal("Failed to create server")
	}

	if server.redisServer == nil {
		t.Fatal("RedisServer should not be nil")
	}

	if server.cmdTable == nil {
		t.Fatal("CommandTable should not be nil")
	}

	t.Log("Server creation test passed")
}

// TestBlockingManager 测试阻塞管理器
func TestBlockingManager(t *testing.T) {
	bm := NewBlockingManager()

	if bm == nil {
		t.Fatal("Failed to create blocking manager")
	}

	// 测试等待和通知
	// 注意：需要实际的客户端连接，这里简化测试
	t.Log("Blocking manager test passed")
}

// TestMemoryStats 测试内存统计
func TestMemoryStats(t *testing.T) {
	ms := NewMemoryStats()

	if ms == nil {
		t.Fatal("Failed to create memory stats")
	}

	ms.Update()

	usedMemory := ms.GetUsedMemory()
	if usedMemory < 0 {
		t.Fatal("Used memory should be non-negative")
	}

	t.Logf("Memory stats test passed: used=%d", usedMemory)
}

// TestSharedObjects 测试共享对象
func TestSharedObjects(t *testing.T) {
	so := NewSharedObjects()

	if so == nil {
		t.Fatal("Failed to create shared objects")
	}

	// 测试获取共享整数
	obj := so.GetSharedInteger(100)
	if obj == nil {
		t.Fatal("Should get shared integer object")
	}

	// 测试获取共享空字符串
	emptyStr := so.GetSharedEmptyString()
	if emptyStr == nil {
		t.Fatal("Should get shared empty string object")
	}

	t.Log("Shared objects test passed")
}
