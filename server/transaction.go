package server

import (
	"sync"

	"github.com/code-100-precent/LingCache/protocol"
)

/*
 * ============================================================================
 * Redis 事务实现
 * ============================================================================
 *
 * Redis 事务支持：
 * - MULTI: 开始事务
 * - EXEC: 执行事务
 * - DISCARD: 取消事务
 * - WATCH: 监视键（乐观锁）
 *
 * 【事务流程】
 * 1. MULTI - 客户端进入事务模式
 * 2. 命令入队 - 所有命令进入队列，不执行
 * 3. EXEC - 执行队列中的所有命令
 * 4. DISCARD - 清空队列，退出事务模式
 */

// Transaction 事务
type Transaction struct {
	commands []*QueuedCommand
	watched  map[string]bool // 监视的键
	mu       sync.Mutex
}

// QueuedCommand 队列中的命令
type QueuedCommand struct {
	cmd  *protocol.RESPValue
	proc CommandProc
}

// NewTransaction 创建新事务
func NewTransaction() *Transaction {
	return &Transaction{
		commands: make([]*QueuedCommand, 0),
		watched:  make(map[string]bool),
	}
}

// AddCommand 添加命令到队列
func (tx *Transaction) AddCommand(cmd *protocol.RESPValue, proc CommandProc) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.commands = append(tx.commands, &QueuedCommand{
		cmd:  cmd,
		proc: proc,
	})
}

// Execute 执行事务
func (tx *Transaction) Execute(ctx *CommandContext) []*protocol.RESPValue {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	results := make([]*protocol.RESPValue, 0, len(tx.commands))

	for _, queuedCmd := range tx.commands {
		// 检查 WATCH 的键是否被修改（简化实现：总是执行）
		// 实际应该检查 watched 键是否被修改

		// 执行命令
		result := queuedCmd.proc(ctx, queuedCmd.cmd.GetArray()[1:])
		results = append(results, result)
	}

	return results
}

// Discard 取消事务
func (tx *Transaction) Discard() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.commands = make([]*QueuedCommand, 0)
	tx.watched = make(map[string]bool)
}

// Watch 监视键
func (tx *Transaction) Watch(key string) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.watched[key] = true
}

// IsWatched 检查键是否被监视
func (tx *Transaction) IsWatched(key string) bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	_, exists := tx.watched[key]
	return exists
}
