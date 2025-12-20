package server

import (
	"errors"

	"github.com/code-100-precent/LingCache/protocol"
	"github.com/code-100-precent/LingCache/storage"
)

/*
 * ============================================================================
 * Redis 命令系统
 * ============================================================================
 *
 * 命令系统负责：
 * 1. 命令注册 - 将命令名称映射到处理函数
 * 2. 命令路由 - 根据命令名称找到对应的处理函数
 * 3. 参数验证 - 验证命令参数
 * 4. 命令执行 - 执行命令并返回结果
 */

var (
	ErrUnknownCommand = errors.New("unknown command")
	ErrWrongArity     = errors.New("wrong number of arguments")
)

// CommandProc 命令处理函数类型
type CommandProc func(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue

// CommandContext 命令执行上下文
type CommandContext struct {
	Server *Server
	Db     *storage.RedisDb
	Client *Client
}

// Command 命令定义
type Command struct {
	Name     string
	Proc     CommandProc
	Arity    int // 参数数量，-N 表示 >= N
	Flags    uint64
	Category string
}

// CommandTable 命令表
type CommandTable struct {
	commands map[string]*Command
}

// NewCommandTable 创建命令表
func NewCommandTable() *CommandTable {
	ct := &CommandTable{
		commands: make(map[string]*Command),
	}
	ct.registerCommands()
	return ct
}

// Register 注册命令
func (ct *CommandTable) Register(cmd *Command) {
	ct.commands[cmd.Name] = cmd
}

// Lookup 查找命令
func (ct *CommandTable) Lookup(name string) (*Command, error) {
	cmd, exists := ct.commands[name]
	if !exists {
		return nil, ErrUnknownCommand
	}
	return cmd, nil
}

// registerCommands 注册所有命令
func (ct *CommandTable) registerCommands() {
	// ========== String 命令 ==========
	ct.Register(&Command{
		Name:     "SET",
		Proc:     cmdSet,
		Arity:    3,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "GET",
		Proc:     cmdGet,
		Arity:    2,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "DEL",
		Proc:     cmdDel,
		Arity:    -2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "EXISTS",
		Proc:     cmdExists,
		Arity:    -2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "TYPE",
		Proc:     cmdType,
		Arity:    2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "EXPIRE",
		Proc:     cmdExpire,
		Arity:    3,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "TTL",
		Proc:     cmdTTL,
		Arity:    2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "EXPIREAT",
		Proc:     cmdExpireAt,
		Arity:    3,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "PEXPIRE",
		Proc:     cmdPExpire,
		Arity:    3,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "PEXPIREAT",
		Proc:     cmdPExpireAt,
		Arity:    3,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "PTTL",
		Proc:     cmdPTTL,
		Arity:    2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "PERSIST",
		Proc:     cmdPersist,
		Arity:    2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "RENAME",
		Proc:     cmdRename,
		Arity:    3,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "RENAMENX",
		Proc:     cmdRenameNx,
		Arity:    3,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "RANDOMKEY",
		Proc:     cmdRandomKey,
		Arity:    1,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "MOVE",
		Proc:     cmdMove,
		Arity:    3,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "OBJECT",
		Proc:     cmdObject,
		Arity:    -2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "SORT",
		Proc:     cmdSort,
		Arity:    -2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "KEYS",
		Proc:     cmdKeys,
		Arity:    2,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "DBSIZE",
		Proc:     cmdDBSize,
		Arity:    1,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "SELECT",
		Proc:     cmdSelect,
		Arity:    2,
		Category: "connection",
	})

	ct.Register(&Command{
		Name:     "FLUSHDB",
		Proc:     cmdFlushDB,
		Arity:    1,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "FLUSHALL",
		Proc:     cmdFlushAll,
		Arity:    1,
		Category: "keyspace",
	})

	ct.Register(&Command{
		Name:     "SCAN",
		Proc:     cmdScan,
		Arity:    -2,
		Category: "keyspace",
	})

	// ========== List 命令 ==========
	ct.Register(&Command{
		Name:     "LPUSH",
		Proc:     cmdLPush,
		Arity:    -3,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "RPUSH",
		Proc:     cmdRPush,
		Arity:    -3,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "LPOP",
		Proc:     cmdLPop,
		Arity:    2,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "RPOP",
		Proc:     cmdRPop,
		Arity:    2,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "LLEN",
		Proc:     cmdLLen,
		Arity:    2,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "LRANGE",
		Proc:     cmdLRange,
		Arity:    4,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "LINDEX",
		Proc:     cmdLIndex,
		Arity:    3,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "LINSERT",
		Proc:     cmdLInsert,
		Arity:    5,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "LREM",
		Proc:     cmdLRem,
		Arity:    4,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "LSET",
		Proc:     cmdLSet,
		Arity:    4,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "LTRIM",
		Proc:     cmdLTrim,
		Arity:    4,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "RPOPLPUSH",
		Proc:     cmdRPopLPush,
		Arity:    3,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "BRPOPLPUSH",
		Proc:     cmdBRPopLPush,
		Arity:    4,
		Category: "list",
	})

	// ========== Set 命令 ==========
	ct.Register(&Command{
		Name:     "SADD",
		Proc:     cmdSAdd,
		Arity:    -3,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SREM",
		Proc:     cmdSRem,
		Arity:    -3,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SMEMBERS",
		Proc:     cmdSMembers,
		Arity:    2,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SCARD",
		Proc:     cmdSCard,
		Arity:    2,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SISMEMBER",
		Proc:     cmdSIsMember,
		Arity:    3,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SINTER",
		Proc:     cmdSInter,
		Arity:    -2,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SUNION",
		Proc:     cmdSUnion,
		Arity:    -2,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SDIFF",
		Proc:     cmdSDiff,
		Arity:    -2,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SPOP",
		Proc:     cmdSPop,
		Arity:    -2,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SRANDMEMBER",
		Proc:     cmdSRandMember,
		Arity:    -2,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SMOVE",
		Proc:     cmdSMove,
		Arity:    4,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SINTERSTORE",
		Proc:     cmdSInterStore,
		Arity:    -3,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SUNIONSTORE",
		Proc:     cmdSUnionStore,
		Arity:    -3,
		Category: "set",
	})

	ct.Register(&Command{
		Name:     "SDIFFSTORE",
		Proc:     cmdSDiffStore,
		Arity:    -3,
		Category: "set",
	})

	// ========== ZSet 命令 ==========
	ct.Register(&Command{
		Name:     "ZADD",
		Proc:     cmdZAdd,
		Arity:    -4,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZREM",
		Proc:     cmdZRem,
		Arity:    -3,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZSCORE",
		Proc:     cmdZScore,
		Arity:    3,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZCARD",
		Proc:     cmdZCard,
		Arity:    2,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZRANGE",
		Proc:     cmdZRange,
		Arity:    -4,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZRANK",
		Proc:     cmdZRank,
		Arity:    3,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZREVRANGE",
		Proc:     cmdZRevRange,
		Arity:    -4,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZREVRANK",
		Proc:     cmdZRevRank,
		Arity:    3,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZINCRBY",
		Proc:     cmdZIncrBy,
		Arity:    4,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZRANGEBYSCORE",
		Proc:     cmdZRangeByScore,
		Arity:    -4,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZCOUNT",
		Proc:     cmdZCount,
		Arity:    4,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZREVRANGEBYSCORE",
		Proc:     cmdZRevRangeByScore,
		Arity:    -4,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZREMRANGEBYRANK",
		Proc:     cmdZRemRangeByRank,
		Arity:    4,
		Category: "sortedset",
	})

	ct.Register(&Command{
		Name:     "ZREMRANGEBYSCORE",
		Proc:     cmdZRemRangeByScore,
		Arity:    4,
		Category: "sortedset",
	})

	// ========== Hash 命令 ==========
	ct.Register(&Command{
		Name:     "HSET",
		Proc:     cmdHSet,
		Arity:    -4,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HGET",
		Proc:     cmdHGet,
		Arity:    3,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HDEL",
		Proc:     cmdHDel,
		Arity:    -3,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HEXISTS",
		Proc:     cmdHExists,
		Arity:    3,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HLEN",
		Proc:     cmdHLen,
		Arity:    2,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HGETALL",
		Proc:     cmdHGetAll,
		Arity:    2,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HKEYS",
		Proc:     cmdHKeys,
		Arity:    2,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HVALS",
		Proc:     cmdHVals,
		Arity:    2,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HINCRBY",
		Proc:     cmdHIncrBy,
		Arity:    4,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HMSET",
		Proc:     cmdHMSet,
		Arity:    -4,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HMGET",
		Proc:     cmdHMGet,
		Arity:    -3,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HSETNX",
		Proc:     cmdHSetNx,
		Arity:    4,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HSTRLEN",
		Proc:     cmdHStrLen,
		Arity:    3,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HINCRBYFLOAT",
		Proc:     cmdHIncrByFloat,
		Arity:    4,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "HSCAN",
		Proc:     cmdHScan,
		Arity:    -3,
		Category: "hash",
	})

	ct.Register(&Command{
		Name:     "MSET",
		Proc:     cmdMSet,
		Arity:    -3,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "MGET",
		Proc:     cmdMGet,
		Arity:    -2,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "SETEX",
		Proc:     cmdSetEx,
		Arity:    4,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "SETNX",
		Proc:     cmdSetNx,
		Arity:    3,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "PSETEX",
		Proc:     cmdPSetEx,
		Arity:    4,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "GETSET",
		Proc:     cmdGetSet,
		Arity:    3,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "APPEND",
		Proc:     cmdAppend,
		Arity:    3,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "STRLEN",
		Proc:     cmdStrLen,
		Arity:    2,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "INCR",
		Proc:     cmdIncr,
		Arity:    2,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "DECR",
		Proc:     cmdDecr,
		Arity:    2,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "INCRBY",
		Proc:     cmdIncrBy,
		Arity:    3,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "DECRBY",
		Proc:     cmdDecrBy,
		Arity:    3,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "GETRANGE",
		Proc:     cmdGetRange,
		Arity:    4,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "SETRANGE",
		Proc:     cmdSetRange,
		Arity:    4,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "SETBIT",
		Proc:     cmdSetBit,
		Arity:    4,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "GETBIT",
		Proc:     cmdGetBit,
		Arity:    3,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "BITCOUNT",
		Proc:     cmdBitCount,
		Arity:    -2,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "BITOP",
		Proc:     cmdBitOp,
		Arity:    -4,
		Category: "string",
	})

	ct.Register(&Command{
		Name:     "BITPOS",
		Proc:     cmdBitPos,
		Arity:    -3,
		Category: "string",
	})

	// ========== 连接命令 ==========
	ct.Register(&Command{
		Name:     "PING",
		Proc:     cmdPing,
		Arity:    -1,
		Category: "connection",
	})

	ct.Register(&Command{
		Name:     "QUIT",
		Proc:     cmdQuit,
		Arity:    1,
		Category: "connection",
	})

	// ========== 服务器命令 ==========
	ct.Register(&Command{
		Name:     "INFO",
		Proc:     cmdInfo,
		Arity:    -1,
		Category: "server",
	})

	ct.Register(&Command{
		Name:     "CONFIG",
		Proc:     cmdConfig,
		Arity:    -2,
		Category: "server",
	})

	// ========== 事务命令 ==========
	ct.Register(&Command{
		Name:     "MULTI",
		Proc:     cmdMulti,
		Arity:    1,
		Category: "transaction",
	})

	ct.Register(&Command{
		Name:     "EXEC",
		Proc:     cmdExec,
		Arity:    1,
		Category: "transaction",
	})

	ct.Register(&Command{
		Name:     "DISCARD",
		Proc:     cmdDiscard,
		Arity:    1,
		Category: "transaction",
	})

	ct.Register(&Command{
		Name:     "WATCH",
		Proc:     cmdWatch,
		Arity:    -2,
		Category: "transaction",
	})

	// ========== 发布订阅命令 ==========
	ct.Register(&Command{
		Name:     "PUBLISH",
		Proc:     cmdPublish,
		Arity:    3,
		Category: "pubsub",
	})

	ct.Register(&Command{
		Name:     "SUBSCRIBE",
		Proc:     cmdSubscribe,
		Arity:    -2,
		Category: "pubsub",
	})

	ct.Register(&Command{
		Name:     "UNSUBSCRIBE",
		Proc:     cmdUnsubscribe,
		Arity:    -1,
		Category: "pubsub",
	})

	ct.Register(&Command{
		Name:     "PSUBSCRIBE",
		Proc:     cmdPSubscribe,
		Arity:    -2,
		Category: "pubsub",
	})

	ct.Register(&Command{
		Name:     "PUNSUBSCRIBE",
		Proc:     cmdPUnsubscribe,
		Arity:    -1,
		Category: "pubsub",
	})

	ct.Register(&Command{
		Name:     "PUBSUB",
		Proc:     cmdPubsub,
		Arity:    -2,
		Category: "pubsub",
	})

	// ========== 阻塞命令 ==========
	ct.Register(&Command{
		Name:     "BLPOP",
		Proc:     cmdBLPop,
		Arity:    -3,
		Category: "list",
	})

	ct.Register(&Command{
		Name:     "BRPOP",
		Proc:     cmdBRPop,
		Arity:    -3,
		Category: "list",
	})

	// ========== 持久化命令 ==========
	ct.Register(&Command{
		Name:     "SAVE",
		Proc:     cmdSave,
		Arity:    1,
		Category: "server",
	})

	ct.Register(&Command{
		Name:     "BGSAVE",
		Proc:     cmdBGSave,
		Arity:    1,
		Category: "server",
	})

	// ========== 集群命令 ==========
	ct.Register(&Command{
		Name:     "CLUSTER",
		Proc:     cmdCluster,
		Arity:    -2,
		Category: "cluster",
	})

	// ========== 复制命令 ==========
	ct.Register(&Command{
		Name:     "REPLCONF",
		Proc:     cmdReplConf,
		Arity:    -2,
		Category: "replication",
	})

	ct.Register(&Command{
		Name:     "PSYNC",
		Proc:     cmdPSync,
		Arity:    -2,
		Category: "replication",
	})

	ct.Register(&Command{
		Name:     "SLAVEOF",
		Proc:     cmdSlaveOf,
		Arity:    -3,
		Category: "replication",
	})

	// ========== AOF 命令 ==========
	ct.Register(&Command{
		Name:     "BGREWRITEAOF",
		Proc:     cmdBGRewriteAOF,
		Arity:    1,
		Category: "server",
	})

	// ========== 阻塞 ZSet 命令 ==========
	ct.Register(&Command{
		Name:     "BZPOPMAX",
		Proc:     cmdBZPopMax,
		Arity:    -3,
		Category: "zset",
	})

	ct.Register(&Command{
		Name:     "BZPOPMIN",
		Proc:     cmdBZPopMin,
		Arity:    -3,
		Category: "zset",
	})
}

// ExecuteCommand 执行命令
func (ct *CommandTable) ExecuteCommand(ctx *CommandContext, req *protocol.RESPValue) *protocol.RESPValue {
	if !req.IsArray() {
		return protocol.NewError("ERR invalid command format")
	}

	array := req.GetArray()
	if len(array) == 0 {
		return protocol.NewError("ERR empty command")
	}

	// 获取命令名称
	cmdName := array[0].ToString()
	if cmdName == "" {
		return protocol.NewError("ERR invalid command name")
	}

	// 转换为大写
	cmdName = toUpper(cmdName)

	// 查找命令
	cmd, err := ct.Lookup(cmdName)
	if err != nil {
		return protocol.NewError("ERR unknown command '" + cmdName + "'")
	}

	// 验证参数数量
	argCount := len(array) - 1 // 减去命令名
	if cmd.Arity > 0 {
		if argCount != cmd.Arity-1 { // Arity 包括命令名
			return protocol.NewError("ERR wrong number of arguments for '" + cmdName + "' command")
		}
	} else if cmd.Arity < 0 {
		minArgs := -cmd.Arity - 1
		if argCount < minArgs {
			return protocol.NewError("ERR wrong number of arguments for '" + cmdName + "' command")
		}
	}

	// 执行命令
	return cmd.Proc(ctx, array[1:])
}

// toUpper 转换为大写（简化实现）
func toUpper(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			result[i] = s[i] - 32
		} else {
			result[i] = s[i]
		}
	}
	return string(result)
}
