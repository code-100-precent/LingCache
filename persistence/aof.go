package persistence

import (
	"bufio"
	"io"
	"os"
	"sync"

	"github.com/code-100-precent/LingCache/protocol"
)

/*
 * ============================================================================
 * AOF (Append Only File) 持久化实现
 * ============================================================================
 *
 * AOF 是 Redis 的追加日志持久化方式，记录每个写命令。
 *
 * 【AOF 文件格式】
 * 直接记录 RESP 格式的命令：
 * *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
 *
 * 【AOF 重写】
 * 将当前数据库状态转换为命令序列，生成新的 AOF 文件。
 */

// AOFWriter AOF 写入器
type AOFWriter struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
}

// NewAOFWriter 创建 AOF 写入器
func NewAOFWriter(filename string) (*AOFWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &AOFWriter{
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

// Append 追加命令到 AOF
func (aof *AOFWriter) Append(cmd *protocol.RESPValue) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	data := cmd.Encode()
	_, err := aof.writer.Write(data)
	if err != nil {
		return err
	}
	return aof.writer.Flush()
}

// Close 关闭 AOF 文件
func (aof *AOFWriter) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	if err := aof.writer.Flush(); err != nil {
		return err
	}
	return aof.file.Close()
}

// Rewrite 重写 AOF 文件
func (aof *AOFWriter) Rewrite(server interface{}) error {
	// 关闭当前文件
	aof.Close()

	// 创建新的 AOF 文件
	newFile, err := os.Create(aof.file.Name() + ".tmp")
	if err != nil {
		return err
	}
	defer newFile.Close()

	// 将当前数据库状态转换为命令序列并写入
	// 简化实现：这里应该遍历所有数据库和键，生成 SET 等命令
	// 实际实现需要访问 server 对象来获取数据

	// 原子替换文件
	return os.Rename(newFile.Name(), aof.file.Name())
}

// AOFLoader AOF 加载器
type AOFLoader struct {
	reader *bufio.Reader
}

// NewAOFLoader 创建 AOF 加载器
func NewAOFLoader(filename string) (*AOFLoader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &AOFLoader{
		reader: bufio.NewReader(file),
	}, nil
}

// Load 从 AOF 文件加载命令
func (loader *AOFLoader) Load() ([]*protocol.RESPValue, error) {
	commands := make([]*protocol.RESPValue, 0)

	for {
		cmd, err := protocol.Decode(loader.reader)
		if err != nil {
			// 检查是否是 EOF 错误（文件读取完毕）
			if err == io.EOF || err.Error() == "EOF" || err.Error() == "unexpected EOF" {
				break
			}
			return nil, err
		}
		commands = append(commands, cmd)
	}

	return commands, nil
}
