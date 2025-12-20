package server

import (
	"bufio"
	"fmt"
	"github.com/code-100-precent/LingCache/persistence"
	"github.com/code-100-precent/LingCache/protocol"
	"github.com/code-100-precent/LingCache/storage"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

/*
 * ============================================================================
 * Redis 服务器实现
 * ============================================================================
 *
 * 服务器负责：
 * 1. 监听 TCP 端口
 * 2. 接受客户端连接
 * 3. 处理客户端请求
 * 4. 返回响应
 */

// Server Redis 服务器
type Server struct {
	addr          string
	redisServer   *storage.RedisServer
	cmdTable      *CommandTable
	listener      net.Listener
	clients       map[*Client]bool
	pubsub        *PubSubManager
	stats         *Stats
	blockingMgr   *BlockingManager
	aofWriter     *persistence.AOFWriter
	sharedObjects *SharedObjects
	memoryStats   *MemoryStats
	rdbFilename   string
	aofFilename   string
	mu            sync.RWMutex
	running       bool
}

// Client 客户端连接
type Client struct {
	conn        net.Conn
	reader      *bufio.Reader
	writer      *bufio.Writer
	server      *Server
	db          *storage.RedisDb
	dbIndex     int // 当前选择的数据库索引
	closed      bool
	transaction *Transaction    // 事务（如果处于事务模式）
	inMulti     bool            // 是否在 MULTI 模式
	pipeline    *PipelineBuffer // 管道缓冲区
}

// NewServer 创建新的服务器
func NewServer(addr string, dbnum int) *Server {
	server := &Server{
		addr:          addr,
		redisServer:   storage.NewRedisServer(dbnum),
		cmdTable:      NewCommandTable(),
		clients:       make(map[*Client]bool),
		pubsub:        NewPubSubManager(),
		stats:         NewStats(),
		blockingMgr:   NewBlockingManager(),
		sharedObjects: NewSharedObjects(),
		memoryStats:   NewMemoryStats(),
		rdbFilename:   "dump.rdb",
		aofFilename:   "appendonly.aof",
		running:       false,
	}

	// 启动定期清理过期阻塞客户端
	go server.cleanBlockingClients()

	return server
}

// InitAOF 初始化 AOF（如果启用）
func (s *Server) InitAOF(aofEnabled bool, aofFilename string) error {
	if !aofEnabled {
		return nil
	}

	s.aofFilename = aofFilename

	// 先加载 AOF 文件恢复数据（如果文件存在）
	if err := s.LoadAOF(aofFilename); err != nil {
		// 如果文件不存在，这是正常的（首次启动）
		if !os.IsNotExist(err) {
			fmt.Printf("Warning: Failed to load AOF file: %v\n", err)
		}
	}

	// 然后创建 AOF writer 用于后续写入
	aofWriter, err := persistence.NewAOFWriter(aofFilename)
	if err != nil {
		return err
	}
	s.aofWriter = aofWriter
	fmt.Printf("AOF initialized: %s\n", aofFilename)
	return nil
}

// LoadAOF 从 AOF 文件加载并重放命令
func (s *Server) LoadAOF(filename string) error {
	// 检查文件是否存在
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return err
	}

	loader, err := persistence.NewAOFLoader(filename)
	if err != nil {
		return err
	}

	commands, err := loader.Load()
	if err != nil {
		return err
	}

	if len(commands) == 0 {
		return nil
	}

	fmt.Printf("Loading AOF file: %s (%d commands)\n", filename, len(commands))

	// 获取默认数据库（数据库 0）
	defaultDb, _ := s.redisServer.GetDb(0)
	currentDbIndex := 0

	// 创建临时上下文用于重放命令（不写入 AOF，避免重复）
	ctx := &CommandContext{
		Server: s,
		Db:     defaultDb,
		Client: nil, // 加载时没有客户端
	}

	// 临时禁用 AOF 写入（避免重复记录）
	originalAofWriter := s.aofWriter
	s.aofWriter = nil

	// 重放所有命令
	for i, cmd := range commands {
		if !cmd.IsArray() || len(cmd.GetArray()) == 0 {
			continue
		}

		cmdArray := cmd.GetArray()
		cmdName := cmdArray[0].ToString()
		if cmdName == "" {
			continue
		}

		cmdName = toUpper(cmdName)

		// 处理 SELECT 命令（切换数据库）
		if cmdName == "SELECT" && len(cmdArray) >= 2 {
			dbIndex, err := strconv.Atoi(cmdArray[1].ToString())
			if err == nil && dbIndex >= 0 && dbIndex < s.redisServer.GetDbNum() {
				currentDbIndex = dbIndex
				db, _ := s.redisServer.GetDb(currentDbIndex)
				ctx.Db = db
			}
			continue
		}

		// 执行命令
		resp := s.cmdTable.ExecuteCommand(ctx, cmd)
		if resp != nil && resp.Type == protocol.RESP_ERROR {
			fmt.Printf("Warning: AOF replay error at command %d (%s): %s\n", i+1, cmdName, resp.Str)
		}
	}

	// 恢复 AOF writer
	s.aofWriter = originalAofWriter

	fmt.Printf("AOF file loaded successfully\n")
	return nil
}

// cleanBlockingClients 定期清理过期的阻塞客户端
func (s *Server) cleanBlockingClients() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !s.running {
			return
		}
		s.blockingMgr.CleanExpired()
	}
}

// Start 启动服务器
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.listener = listener
	s.running = true

	fmt.Printf("Redis server started on %s\n", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if !s.running {
				return nil
			}
			continue
		}

		// 每个客户端默认使用数据库 0
		defaultDb, _ := s.redisServer.GetDb(0)
		client := &Client{
			conn:        conn,
			reader:      bufio.NewReader(conn),
			writer:      bufio.NewWriter(conn),
			server:      s,
			db:          defaultDb,
			dbIndex:     0,
			closed:      false,
			transaction: nil,
			inMulti:     false,
			pipeline:    NewPipelineBuffer(),
		}

		s.mu.Lock()
		s.clients[client] = true
		s.mu.Unlock()

		go s.handleClient(client)
	}
}

// Stop 停止服务器
func (s *Server) Stop() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}

	s.mu.Lock()
	for client := range s.clients {
		client.Close()
	}
	s.clients = make(map[*Client]bool)
	s.mu.Unlock()
}

// handleClient 处理客户端连接
func (s *Server) handleClient(client *Client) {
	defer client.Close()

	for {
		// 读取请求
		req, err := protocol.Decode(client.reader)
		if err != nil {
			if client.closed {
				return
			}
			// 发送错误响应
			resp := protocol.NewError("ERR " + err.Error())
			client.writeResponse(resp)
			return
		}

		// 创建命令上下文
		ctx := &CommandContext{
			Server: s,
			Db:     client.db,
			Client: client,
		}

		// 检查是否在事务模式
		if client.inMulti {
			// 事务模式：命令入队
			cmdName := req.GetArray()[0].ToString()
			cmdName = toUpper(cmdName)

			// 某些命令不能在事务中执行
			if cmdName == "EXEC" || cmdName == "DISCARD" || cmdName == "WATCH" || cmdName == "MULTI" {
				// 这些命令直接执行
				resp := s.cmdTable.ExecuteCommand(ctx, req)
				if resp != nil {
					if err := client.writeResponse(resp); err != nil {
						return
					}
				}
				continue
			}

			// 其他命令入队
			cmd, err := s.cmdTable.Lookup(cmdName)
			if err != nil {
				resp := protocol.NewError("ERR " + err.Error())
				if err := client.writeResponse(resp); err != nil {
					return
				}
				continue
			}

			if client.transaction == nil {
				client.transaction = NewTransaction()
			}
			client.transaction.AddCommand(req, cmd.Proc)

			// 返回 QUEUED
			resp := protocol.NewSimpleString("QUEUED")
			if err := client.writeResponse(resp); err != nil {
				return
			}
			continue
		}

		// 正常模式：执行命令
		startTime := time.Now()
		resp := s.cmdTable.ExecuteCommand(ctx, req)
		duration := time.Since(startTime)

		// 记录统计信息
		if len(req.GetArray()) > 0 {
			cmdName := req.GetArray()[0].ToString()
			cmdName = toUpper(cmdName) // 转换为大写
			s.stats.RecordCommand(cmdName, duration)

			// 如果是写命令且 AOF 已启用，写入 AOF
			if s.aofWriter != nil && s.isWriteCommand(cmdName) && resp != nil && resp.Type != protocol.RESP_ERROR {
				// 写入 AOF（使用原始请求）
				if err := s.aofWriter.Append(req); err != nil {
					// AOF 写入失败，记录错误但不影响命令执行
					fmt.Printf("AOF write error: %v\n", err)
				}
			}
		}

		// 发送响应（某些命令如 SUBSCRIBE 可能返回 nil）
		if resp != nil {
			if err := client.writeResponse(resp); err != nil {
				return
			}
		}
	}
}

// writeResponse 写入响应
func (c *Client) writeResponse(resp *protocol.RESPValue) error {
	data := resp.Encode()
	_, err := c.writer.Write(data)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// Close 关闭客户端连接
func (c *Client) Close() {
	if c.closed {
		return
	}

	c.closed = true
	c.conn.Close()

	c.server.mu.Lock()
	delete(c.server.clients, c)
	c.server.mu.Unlock()
}

// isWriteCommand 判断是否是写命令
func (s *Server) isWriteCommand(cmdName string) bool {
	writeCommands := map[string]bool{
		"SET": true, "MSET": true, "SETEX": true, "SETNX": true,
		"DEL": true, "EXPIRE": true, "EXPIREAT": true, "PERSIST": true,
		"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
		"LREM": true, "LSET": true, "LTRIM": true,
		"SADD": true, "SREM": true, "SPOP": true,
		"ZADD": true, "ZREM": true, "ZINCRBY": true,
		"HSET": true, "HMSET": true, "HDEL": true, "HINCRBY": true,
		"INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true,
		"APPEND": true, "GETSET": true,
	}
	return writeCommands[cmdName]
}

// GetRedisServer 获取 Redis 服务器实例
func (s *Server) GetRedisServer() *storage.RedisServer {
	return s.redisServer
}
