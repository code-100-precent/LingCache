package cluster

import (
	"github.com/code-100-precent/LingCache/storage"
	"testing"
)

// TestClusterCreation 测试集群创建
func TestClusterCreation(t *testing.T) {
	server := storage.NewRedisServer(16)
	cluster := NewCluster(server, "node1", "127.0.0.1:7000")

	if cluster == nil {
		t.Fatal("Failed to create cluster")
	}

	if cluster.GetMyself().NodeID != "node1" {
		t.Fatal("NodeID mismatch")
	}

	t.Log("Cluster creation test passed")
}

// TestHashSlot 测试哈希槽计算
func TestHashSlot(t *testing.T) {
	// 测试哈希槽计算的一致性
	key1 := "test:key:1"
	key2 := "test:key:2"

	slot1 := HashSlot(key1)
	slot2 := HashSlot(key2)

	// 验证槽号在有效范围内
	if slot1 < 0 || slot1 >= CLUSTER_SLOTS {
		t.Fatalf("Invalid slot: %d", slot1)
	}

	if slot2 < 0 || slot2 >= CLUSTER_SLOTS {
		t.Fatalf("Invalid slot: %d", slot2)
	}

	// 相同键应该得到相同槽
	slot1Again := HashSlot(key1)
	if slot1 != slot1Again {
		t.Fatal("Hash slot should be consistent")
	}

	t.Logf("Hash slot test passed: key1=%d, key2=%d", slot1, slot2)
}

// TestSlotAssignment 测试槽分配
func TestSlotAssignment(t *testing.T) {
	server := storage.NewRedisServer(16)
	cluster := NewCluster(server, "node1", "127.0.0.1:7000")

	// 添加节点
	cluster.AddNode("node1", "127.0.0.1:7000")
	cluster.AddNode("node2", "127.0.0.1:7001")

	// 分配槽
	slots1 := []int{0, 1, 2, 3, 4}
	slots2 := []int{5, 6, 7, 8, 9}

	cluster.AssignSlots("node1", slots1)
	cluster.AssignSlots("node2", slots2)

	// 验证槽分配
	for _, slot := range slots1 {
		node := cluster.GetSlotNode(slot)
		if node == nil || node.NodeID != "node1" {
			t.Fatalf("Slot %d should be assigned to node1", slot)
		}
	}

	for _, slot := range slots2 {
		node := cluster.GetSlotNode(slot)
		if node == nil || node.NodeID != "node2" {
			t.Fatalf("Slot %d should be assigned to node2", slot)
		}
	}

	t.Log("Slot assignment test passed")
}

// TestFailoverManager 测试故障转移管理器
func TestFailoverManager(t *testing.T) {
	server := storage.NewRedisServer(16)
	cluster := NewCluster(server, "node1", "127.0.0.1:7000")

	failoverMgr := cluster.GetFailoverManager()
	if failoverMgr == nil {
		t.Fatal("Failover manager should not be nil")
	}

	// 注册节点
	failoverMgr.RegisterNode("node1", true, []int{0, 1, 2}, []string{"node2"})
	failoverMgr.RegisterNode("node2", false, []int{}, []string{})

	// 更新节点状态
	failoverMgr.UpdateNodeStatus("node1", true)
	failoverMgr.UpdateNodeStatus("node2", true)

	t.Log("Failover manager test passed")
}
