package replication

import (
	"bufio"
	"fmt"
	"net"

	"github.com/code-100-precent/LingCache/protocol"
)

/*
 * ============================================================================
 * Redis 主从复制实现（从节点）
 * ============================================================================
 *
 * 从节点连接到主节点，接收数据同步。
 */

// Slave 从节点
type Slave struct {
	masterAddr string
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	running    bool
}

// NewSlave 创建从节点
func NewSlave(masterAddr string) *Slave {
	return &Slave{
		masterAddr: masterAddr,
		running:    false,
	}
}

// Connect 连接到主节点
func (s *Slave) Connect() error {
	conn, err := net.Dial("tcp", s.masterAddr)
	if err != nil {
		return err
	}

	s.conn = conn
	s.reader = bufio.NewReader(conn)
	s.writer = bufio.NewWriter(conn)
	s.running = true

	// 发送 PING
	s.writer.WriteString("*1\r\n$4\r\nPING\r\n")
	s.writer.Flush()

	// 发送 REPLCONF
	s.writer.WriteString("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n")
	s.writer.Flush()

	// 发送 PSYNC
	s.writer.WriteString("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	s.writer.Flush()

	// 启动接收线程
	go s.receiveCommands()

	return nil
}

// receiveCommands 接收主节点的命令
func (s *Slave) receiveCommands() {
	for s.running {
		cmd, err := protocol.Decode(s.reader)
		if err != nil {
			fmt.Printf("Slave receive error: %v\n", err)
			break
		}

		// 处理命令（简化实现：只打印）
		fmt.Printf("Slave received: %s\n", string(cmd.Encode()))
	}
}

// Close 关闭连接
func (s *Slave) Close() {
	s.running = false
	if s.conn != nil {
		s.conn.Close()
	}
}
