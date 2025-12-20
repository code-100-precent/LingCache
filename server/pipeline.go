package server

import (
	"sync"

	"github.com/code-100-precent/LingCache/protocol"
)

/*
 * ============================================================================
 * 管道支持（Pipeline）
 * ============================================================================
 *
 * Redis Pipeline 允许客户端发送多个命令而不等待响应，
 * 然后一次性接收所有响应。
 *
 * 实现方式：
 * - 客户端可以连续发送多个命令
 * - 服务器批量执行并返回结果
 */

// PipelineBuffer 管道缓冲区
type PipelineBuffer struct {
	commands []*protocol.RESPValue
	results  []*protocol.RESPValue
	mu       sync.Mutex
}

// NewPipelineBuffer 创建管道缓冲区
func NewPipelineBuffer() *PipelineBuffer {
	return &PipelineBuffer{
		commands: make([]*protocol.RESPValue, 0),
		results:  make([]*protocol.RESPValue, 0),
	}
}

// AddCommand 添加命令到管道
func (pb *PipelineBuffer) AddCommand(cmd *protocol.RESPValue) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.commands = append(pb.commands, cmd)
}

// Execute 执行管道中的所有命令
func (pb *PipelineBuffer) Execute(ctx *CommandContext) []*protocol.RESPValue {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	results := make([]*protocol.RESPValue, 0, len(pb.commands))

	for _, cmd := range pb.commands {
		resp := ctx.Server.cmdTable.ExecuteCommand(ctx, cmd)
		results = append(results, resp)
	}

	// 清空命令列表
	pb.commands = make([]*protocol.RESPValue, 0)

	return results
}

// Clear 清空管道
func (pb *PipelineBuffer) Clear() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.commands = make([]*protocol.RESPValue, 0)
	pb.results = make([]*protocol.RESPValue, 0)
}
