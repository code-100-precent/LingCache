package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/code-100-precent/LingCache/storage"
)

/*
 * ============================================================================
 * Redis 集群数据迁移集成 - Data Migration Integration
 * ============================================================================
 *
 * 【核心原理】
 * 实际的数据迁移需要与存储层集成，从源节点的数据库中获取键值对，
 * 然后通过网络传输到目标节点，并在目标节点创建。
 *
 * 1. MIGRATE 命令实现
 *    MIGRATE <host> <port> <key> <destination-db> <timeout> [COPY] [REPLACE]
 *    - 原子性地将键从源节点迁移到目标节点
 *    - 如果指定 COPY，源节点保留键
 *    - 如果指定 REPLACE，目标节点覆盖已存在的键
 *
 * 2. 迁移流程
 *    a) 序列化键值对：将 RedisObject 序列化为网络格式
 *    b) 网络传输：通过 TCP 连接发送到目标节点
 *    c) 反序列化：目标节点反序列化并创建对象
 *    d) 删除源键：如果未指定 COPY，删除源节点的键
 *
 * 3. 批量迁移
 *    - 使用管道批量迁移多个键
 *    - 减少网络往返次数
 *    - 提高迁移效率
 *
 * 【面试题】
 * Q1: MIGRATE 命令如何保证原子性？
 * A1: 原子性保证：
 *     - 键要么在源节点，要么在目标节点，不会同时存在
 *     - 如果迁移失败，键仍在源节点
 *     - 使用事务或两阶段提交确保一致性
 *     实现方式：
 *     1. 在源节点标记键为迁移中
 *     2. 传输数据到目标节点
 *     3. 目标节点确认接收
 *     4. 源节点删除键
 *     如果任何步骤失败，回滚操作
 *
 * Q2: 迁移大键（Big Key）时如何处理？
 * A2: 大键处理策略：
 *     1. **分块传输**：将大键分成多个块传输
 *     2. **流式传输**：使用流式传输，避免内存占用过大
 *     3. **超时处理**：设置合理的超时时间
 *     4. **进度跟踪**：跟踪传输进度，支持断点续传
 *     5. **压缩传输**：对大键进行压缩，减少网络传输
 *     例如：10MB 的字符串，可以分成 1MB 的块传输
 *
 * Q3: 迁移过程中如何处理键的过期时间？
 * A3: 过期时间处理：
 *     1. **传输过期时间**：将过期时间一起传输
 *     2. **相对时间**：使用相对时间（TTL）而不是绝对时间
 *     3. **目标节点设置**：在目标节点设置相同的过期时间
 *     4. **过期检查**：迁移前检查键是否过期
 *     如果键在迁移过程中过期：
 *     - 源节点：删除键，返回错误
 *     - 目标节点：不创建键
 *
 * Q4: 迁移失败如何回滚？
 * A4: 回滚机制：
 *     1. **记录迁移状态**：记录哪些键正在迁移
 *     2. **失败检测**：检测迁移失败（网络错误、超时等）
 *     3. **回滚操作**：将已迁移的键迁移回源节点
 *     4. **清理状态**：清理迁移状态标记
 *     部分迁移失败：
 *     - 已迁移的键在目标节点
 *     - 未迁移的键在源节点
 *     - 需要手动处理或自动回滚
 */

// MigrationClient 迁移客户端（用于与目标节点通信）
type MigrationClient struct {
	conn    net.Conn
	host    string
	port    int
	timeout time.Duration
}

// NewMigrationClient 创建迁移客户端
func NewMigrationClient(host string, port int, timeout time.Duration) (*MigrationClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}

	return &MigrationClient{
		conn:    conn,
		host:    host,
		port:    port,
		timeout: timeout,
	}, nil
}

// MigrateKey 迁移单个键
func (mc *MigrationClient) MigrateKey(key string, obj *storage.RedisObject, destDb int, copy bool) error {
	// 序列化对象
	data, err := serializeObject(obj)
	if err != nil {
		return err
	}

	// 构建 MIGRATE 命令
	cmd := buildMigrateCommand(key, destDb, data, copy)

	// 发送命令
	if _, err := mc.conn.Write([]byte(cmd)); err != nil {
		return err
	}

	// 读取响应
	response := make([]byte, 1024)
	n, err := mc.conn.Read(response)
	if err != nil {
		return err
	}

	// 解析响应
	if string(response[:n]) != "+OK\r\n" {
		return errors.New("migration failed")
	}

	return nil
}

// Close 关闭连接
func (mc *MigrationClient) Close() error {
	if mc.conn != nil {
		return mc.conn.Close()
	}
	return nil
}

// serializeObject 序列化 RedisObject
func serializeObject(obj *storage.RedisObject) ([]byte, error) {
	// 简化实现：使用 JSON 序列化
	// 实际应该使用 Redis 的序列化格式（RDB 格式）
	data := map[string]interface{}{
		"type":     obj.Type,
		"encoding": obj.Encoding,
		"data":     obj.Ptr,
	}

	return json.Marshal(data)
}

// buildMigrateCommand 构建 MIGRATE 命令
func buildMigrateCommand(key string, destDb int, data []byte, copy bool) string {
	// 简化实现：实际应该使用 Redis 协议格式
	cmd := fmt.Sprintf("MIGRATE %s %d %d", key, destDb, 5000)
	if copy {
		cmd += " COPY"
	}
	cmd += "\r\n"
	return cmd
}

// GetKeysInSlot 获取槽中的所有键（与存储层集成）
func GetKeysInSlot(server *storage.RedisServer, slot int) []string {
	keys := make([]string, 0)

	// 遍历所有数据库
	for i := 0; i < server.GetDbNum(); i++ {
		db, err := server.GetDb(i)
		if err != nil {
			continue
		}

		// 获取数据库中的所有键
		allKeys := db.Keys("*")

		// 过滤出属于指定槽的键
		for _, key := range allKeys {
			if HashSlot(key) == slot {
				keys = append(keys, key)
			}
		}
	}

	return keys
}

// MigrateSlotData 迁移槽中的所有数据（与存储层集成）
func (rm *ReshardingManager) MigrateSlotData(slot int, sourceNodeID, targetNodeID string, server *storage.RedisServer) error {
	// 开始迁移
	if err := rm.StartMigration(slot, sourceNodeID, targetNodeID); err != nil {
		return err
	}

	// 获取目标节点
	rm.cluster.mu.RLock()
	targetNode, exists := rm.cluster.nodes[targetNodeID]
	rm.cluster.mu.RUnlock()
	if !exists {
		return errors.New("target node not found")
	}

	// 获取槽中的所有键
	keys := GetKeysInSlot(server, slot)

	// 更新迁移总数
	rm.mu.Lock()
	if migration, exists := rm.migrations[slot]; exists {
		migration.KeysTotal = len(keys)
	}
	rm.mu.Unlock()

	// 创建迁移客户端
	client, err := NewMigrationClient(targetNode.Addr, 6379, 10*time.Second)
	if err != nil {
		return err
	}
	defer client.Close()

	// 迁移每个键
	for _, key := range keys {
		// 从存储层获取对象
		obj, err := getObjectFromStorage(server, key)
		if err != nil {
			fmt.Printf("Failed to get key %s: %v\n", key, err)
			continue
		}

		// 迁移键
		if err := client.MigrateKey(key, obj, 0, false); err != nil {
			fmt.Printf("Failed to migrate key %s: %v\n", key, err)
			continue
		}

		// 从源节点删除键（如果未指定 COPY）
		deleteKeyFromStorage(server, key)

		// 更新迁移进度
		rm.mu.Lock()
		if migration, exists := rm.migrations[slot]; exists {
			migration.KeysMigrated++
		}
		rm.mu.Unlock()
	}

	// 完成迁移
	return rm.CompleteMigration(slot)
}

// getObjectFromStorage 从存储层获取对象
func getObjectFromStorage(server *storage.RedisServer, key string) (*storage.RedisObject, error) {
	// 遍历所有数据库查找键
	for i := 0; i < server.GetDbNum(); i++ {
		db, err := server.GetDb(i)
		if err != nil {
			continue
		}

		obj, err := db.Get(key)
		if err == nil {
			return obj, nil
		}
	}

	return nil, errors.New("key not found")
}

// deleteKeyFromStorage 从存储层删除键
func deleteKeyFromStorage(server *storage.RedisServer, key string) {
	// 遍历所有数据库删除键
	for i := 0; i < server.GetDbNum(); i++ {
		db, err := server.GetDb(i)
		if err != nil {
			continue
		}

		db.Del(key)
	}
}
