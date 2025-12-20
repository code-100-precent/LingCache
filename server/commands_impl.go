package server

import (
	"fmt"
	"github.com/code-100-precent/LingCache/persistence"
	"github.com/code-100-precent/LingCache/protocol"
	"github.com/code-100-precent/LingCache/storage"
	"github.com/code-100-precent/LingCache/structure"
	"strconv"
	"strings"
	"time"
)

// ========== String 命令实现 ==========

func cmdSet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	value := args[1].ToString()

	obj := storage.NewStringObject([]byte(value))
	ctx.Db.Set(key, obj)

	return protocol.NewSimpleString("OK")
}

func cmdGet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	val, err := obj.GetStringValue()
	if err != nil {
		return protocol.NewError("ERR " + err.Error())
	}

	return protocol.NewBulkString(string(val))
}

func cmdMSet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args)%2 != 0 {
		return protocol.NewError("ERR wrong number of arguments for MSET")
	}

	for i := 0; i < len(args); i += 2 {
		key := args[i].ToString()
		value := args[i+1].ToString()
		obj := storage.NewStringObject([]byte(value))
		ctx.Db.Set(key, obj)
	}

	return protocol.NewSimpleString("OK")
}

func cmdMGet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	results := make([]*protocol.RESPValue, len(args))

	for i, arg := range args {
		key := arg.ToString()
		obj, err := ctx.Db.Get(key)
		if err != nil {
			results[i] = protocol.NewNullBulkString()
		} else {
			val, _ := obj.GetStringValue()
			results[i] = protocol.NewBulkString(string(val))
		}
	}

	return protocol.NewArray(results)
}

// ========== 通用命令实现 ==========

func cmdDel(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	count := 0
	for _, arg := range args {
		key := arg.ToString()
		if ctx.Db.Del(key) {
			count++
		}
	}
	return protocol.NewInteger(int64(count))
}

func cmdExists(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	count := 0
	for _, arg := range args {
		key := arg.ToString()
		if ctx.Db.Exists(key) {
			count++
		}
	}
	return protocol.NewInteger(int64(count))
}

func cmdType(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	typ, err := ctx.Db.Type(key)
	if err != nil {
		return protocol.NewSimpleString("none")
	}

	return protocol.NewSimpleString(typ)
}

func cmdExpire(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	seconds, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	if ctx.Db.Expire(key, seconds) {
		return protocol.NewInteger(1)
	}
	return protocol.NewInteger(0)
}

func cmdTTL(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	ttl, err := ctx.Db.TTL(key)
	if err != nil {
		return protocol.NewInteger(-2) // 键不存在
	}

	return protocol.NewInteger(ttl)
}

func cmdKeys(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	pattern := args[0].ToString()
	keys := ctx.Db.Keys(pattern)

	results := make([]*protocol.RESPValue, len(keys))
	for i, key := range keys {
		results[i] = protocol.NewBulkString(key)
	}

	return protocol.NewArray(results)
}

func cmdDBSize(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	size := ctx.Db.DBSize()
	return protocol.NewInteger(int64(size))
}

func cmdSelect(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	dbIndex, err := strconv.Atoi(args[0].ToString())
	if err != nil {
		return protocol.NewError("ERR invalid DB index")
	}

	// 验证数据库索引
	redisServer := ctx.Server.GetRedisServer()
	if dbIndex < 0 || dbIndex >= redisServer.GetDbNum() {
		return protocol.NewError("ERR invalid DB index")
	}

	// 获取指定的数据库
	newDb, err := redisServer.GetDb(dbIndex)
	if err != nil {
		return protocol.NewError("ERR invalid DB index")
	}

	// 更新客户端的数据库（每个客户端有独立的数据库选择）
	ctx.Client.db = newDb
	ctx.Client.dbIndex = dbIndex

	return protocol.NewSimpleString("OK")
}

func cmdFlushDB(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	ctx.Db.FlushDB()
	return protocol.NewSimpleString("OK")
}

func cmdFlushAll(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	ctx.Server.GetRedisServer().FlushAll()
	return protocol.NewSimpleString("OK")
}

func cmdScan(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	// SCAN cursor [MATCH pattern] [COUNT count]
	cursor, err := strconv.ParseInt(args[0].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR invalid cursor")
	}

	// 解析可选参数
	pattern := "*"
	count := int64(10)

	for i := 1; i < len(args); i++ {
		arg := args[i].ToString()
		if arg == "MATCH" && i+1 < len(args) {
			pattern = args[i+1].ToString()
			i++
		} else if arg == "COUNT" && i+1 < len(args) {
			if c, err := strconv.ParseInt(args[i+1].ToString(), 10, 64); err == nil {
				count = c
			}
			i++
		}
	}

	// 获取所有键
	allKeys := ctx.Db.Keys(pattern)

	// 分页处理
	start := int(cursor)
	if start >= len(allKeys) {
		// 扫描完成
		return protocol.NewArray([]*protocol.RESPValue{
			protocol.NewBulkString("0"),                // 新的 cursor (0 表示完成)
			protocol.NewArray([]*protocol.RESPValue{}), // 空的键列表
		})
	}

	end := start + int(count)
	if end > len(allKeys) {
		end = len(allKeys)
	}

	keys := allKeys[start:end]
	nextCursor := int64(end)
	if end >= len(allKeys) {
		nextCursor = 0 // 扫描完成
	}

	// 构建响应
	keyValues := make([]*protocol.RESPValue, len(keys))
	for i, key := range keys {
		keyValues[i] = protocol.NewBulkString(key)
	}

	return protocol.NewArray([]*protocol.RESPValue{
		protocol.NewBulkString(strconv.FormatInt(nextCursor, 10)),
		protocol.NewArray(keyValues),
	})
}

// ========== List 命令实现 ==========

func cmdLPush(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	var list *structure.RedisList
	if err != nil {
		// 创建新的 List
		listObj := storage.NewListObject()
		ctx.Db.Set(key, listObj)
		list, _ = listObj.GetList()
	} else {
		list, err = obj.GetList()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
	}

	count := 0
	for i := 1; i < len(args); i++ {
		value := args[i].ToString()
		list.Push([]byte(value), 0) // HEAD
		count++
	}

	// 记录到 AOF
	if ctx.Server.aofWriter != nil {
		cmd := protocol.NewArray(args)
		ctx.Server.aofWriter.Append(cmd)
	}

	// 通知阻塞的客户端
	if list.Len() > 0 {
		ctx.Server.blockingMgr.Notify(key, protocol.NewArray([]*protocol.RESPValue{
			protocol.NewBulkString(key),
			protocol.NewBulkString(string(args[1].ToString())),
		}))
	}

	return protocol.NewInteger(int64(list.Len()))
}

func cmdRPush(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	var list *structure.RedisList
	if err != nil {
		// 创建新的 List
		listObj := storage.NewListObject()
		ctx.Db.Set(key, listObj)
		list, _ = listObj.GetList()
	} else {
		list, err = obj.GetList()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
	}

	count := 0
	for i := 1; i < len(args); i++ {
		value := args[i].ToString()
		list.Push([]byte(value), 1) // TAIL
		count++
	}

	// 记录到 AOF
	if ctx.Server.aofWriter != nil {
		cmd := protocol.NewArray(args)
		ctx.Server.aofWriter.Append(cmd)
	}

	// 通知阻塞的客户端
	if list.Len() > 0 {
		ctx.Server.blockingMgr.Notify(key, protocol.NewArray([]*protocol.RESPValue{
			protocol.NewBulkString(key),
			protocol.NewBulkString(string(args[1].ToString())),
		}))
	}

	return protocol.NewInteger(int64(list.Len()))
}

func cmdLPop(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	value, err := list.Pop(0) // HEAD
	if err != nil {
		return protocol.NewNullBulkString()
	}

	return protocol.NewBulkString(string(value))
}

func cmdRPop(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	value, err := list.Pop(1) // TAIL
	if err != nil {
		return protocol.NewNullBulkString()
	}

	return protocol.NewBulkString(string(value))
}

func cmdLLen(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	return protocol.NewInteger(int64(list.Len()))
}

func cmdLRange(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	start, _ := strconv.Atoi(args[1].ToString())
	end, _ := strconv.Atoi(args[2].ToString())

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	values, err := list.Range(start, end)
	if err != nil {
		return protocol.NewError("ERR " + err.Error())
	}

	results := make([]*protocol.RESPValue, len(values))
	for i, v := range values {
		results[i] = protocol.NewBulkString(string(v))
	}

	return protocol.NewArray(results)
}

// ========== Set 命令实现 ==========

func cmdSAdd(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	var set *structure.RedisSet
	if err != nil {
		// 创建新的 Set
		setObj := storage.NewSetObject()
		ctx.Db.Set(key, setObj)
		set, _ = setObj.GetSet()
	} else {
		set, err = obj.GetSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
	}

	count := 0
	for i := 1; i < len(args); i++ {
		member := args[i].ToString()
		if set.Add([]byte(member)) == nil {
			count++
		}
	}

	return protocol.NewInteger(int64(count))
}

func cmdSRem(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	set, err := obj.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	count := 0
	for i := 1; i < len(args); i++ {
		member := args[i].ToString()
		if set.Remove([]byte(member)) == nil {
			count++
		}
	}

	return protocol.NewInteger(int64(count))
}

func cmdSMembers(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	set, err := obj.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	members := set.Members()
	results := make([]*protocol.RESPValue, len(members))
	for i, member := range members {
		results[i] = protocol.NewBulkString(string(member))
	}

	return protocol.NewArray(results)
}

func cmdSCard(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	set, err := obj.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	return protocol.NewInteger(int64(set.Card()))
}

func cmdSIsMember(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	member := args[1].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	set, err := obj.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	if set.IsMember([]byte(member)) {
		return protocol.NewInteger(1)
	}
	return protocol.NewInteger(0)
}

func cmdSInter(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) == 0 {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	// 获取第一个集合
	key1 := args[0].ToString()
	obj1, err := ctx.Db.Get(key1)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	set1, err := obj1.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 获取其他集合
	others := make([]*structure.RedisSet, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		key := args[i].ToString()
		obj, err := ctx.Db.Get(key)
		if err != nil {
			return protocol.NewArray([]*protocol.RESPValue{})
		}

		set, err := obj.GetSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		others = append(others, set)
	}

	// 求交集
	result := set1.Inter(others...)
	members := result.Members()

	results := make([]*protocol.RESPValue, len(members))
	for i, member := range members {
		results[i] = protocol.NewBulkString(string(member))
	}

	return protocol.NewArray(results)
}

func cmdSUnion(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) == 0 {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	// 获取第一个集合
	key1 := args[0].ToString()
	obj1, err := ctx.Db.Get(key1)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	set1, err := obj1.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 获取其他集合
	others := make([]*structure.RedisSet, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		key := args[i].ToString()
		obj, err := ctx.Db.Get(key)
		if err != nil {
			continue
		}

		set, err := obj.GetSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		others = append(others, set)
	}

	// 求并集
	result := set1.Union(others...)
	members := result.Members()

	results := make([]*protocol.RESPValue, len(members))
	for i, member := range members {
		results[i] = protocol.NewBulkString(string(member))
	}

	return protocol.NewArray(results)
}

func cmdSDiff(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) == 0 {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	// 获取第一个集合
	key1 := args[0].ToString()
	obj1, err := ctx.Db.Get(key1)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	set1, err := obj1.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 获取其他集合
	others := make([]*structure.RedisSet, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		key := args[i].ToString()
		obj, err := ctx.Db.Get(key)
		if err != nil {
			continue
		}

		set, err := obj.GetSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		others = append(others, set)
	}

	// 求差集
	result := set1.Diff(others...)
	members := result.Members()

	results := make([]*protocol.RESPValue, len(members))
	for i, member := range members {
		results[i] = protocol.NewBulkString(string(member))
	}

	return protocol.NewArray(results)
}

// ========== ZSet 命令实现 ==========

func cmdZAdd(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	var zset *structure.RedisZSet
	if err != nil {
		// 创建新的 ZSet
		zsetObj := storage.NewZSetObject()
		ctx.Db.Set(key, zsetObj)
		zset, _ = zsetObj.GetZSet()
	} else {
		zset, err = obj.GetZSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
	}

	count := 0
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		score, err := strconv.ParseFloat(args[i].ToString(), 64)
		if err != nil {
			return protocol.NewError("ERR value is not a valid float")
		}
		member := args[i+1].ToString()
		if zset.Add([]byte(member), score) == nil {
			count++
		}
	}

	return protocol.NewInteger(int64(count))
}

func cmdZRem(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	count := 0
	for i := 1; i < len(args); i++ {
		member := args[i].ToString()
		if zset.Remove([]byte(member)) == nil {
			count++
		}
	}

	return protocol.NewInteger(int64(count))
}

func cmdZScore(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	member := args[1].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	score, exists := zset.Score([]byte(member))
	if !exists {
		return protocol.NewNullBulkString()
	}

	return protocol.NewBulkString(strconv.FormatFloat(score, 'f', -1, 64))
}

func cmdZCard(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	return protocol.NewInteger(int64(zset.Card()))
}

func cmdZRange(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	start, _ := strconv.Atoi(args[1].ToString())
	end, _ := strconv.Atoi(args[2].ToString())

	reverse := false
	if len(args) > 3 {
		withScores := args[3].ToString()
		if withScores == "WITHSCORES" {
			// 简化实现：返回带分数的结果
		}
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	entries, err := zset.Range(start, end, reverse)
	if err != nil {
		return protocol.NewError("ERR " + err.Error())
	}

	results := make([]*protocol.RESPValue, 0, len(entries)*2)
	for _, entry := range entries {
		results = append(results, protocol.NewBulkString(string(entry.Member())))
		results = append(results, protocol.NewBulkString(strconv.FormatFloat(entry.Score(), 'f', -1, 64)))
	}

	return protocol.NewArray(results)
}

func cmdZRank(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	member := args[1].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	rank, exists := zset.Rank([]byte(member), false)
	if !exists {
		return protocol.NewNullBulkString()
	}

	return protocol.NewInteger(int64(rank))
}

// ========== Hash 命令实现 ==========

func cmdHSet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	var hash *structure.RedisHash
	if err != nil {
		// 创建新的 Hash
		hashObj := storage.NewHashObject()
		ctx.Db.Set(key, hashObj)
		hash, _ = hashObj.GetHash()
	} else {
		hash, err = obj.GetHash()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
	}

	count := 0
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		field := args[i].ToString()
		value := args[i+1].ToString()
		if hash.Set([]byte(field), []byte(value)) == nil {
			count++
		}
	}

	return protocol.NewInteger(int64(count))
}

func cmdHGet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	field := args[1].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	value, exists := hash.Get([]byte(field))
	if !exists {
		return protocol.NewNullBulkString()
	}

	return protocol.NewBulkString(string(value))
}

func cmdHDel(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	count := 0
	for i := 1; i < len(args); i++ {
		field := args[i].ToString()
		if hash.Del([]byte(field)) == nil {
			count++
		}
	}

	return protocol.NewInteger(int64(count))
}

func cmdHExists(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	field := args[1].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	if hash.Exists([]byte(field)) {
		return protocol.NewInteger(1)
	}
	return protocol.NewInteger(0)
}

func cmdHLen(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	return protocol.NewInteger(int64(hash.Len()))
}

func cmdHGetAll(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	entries := hash.GetAll()
	results := make([]*protocol.RESPValue, 0, len(entries)*2)
	for _, entry := range entries {
		results = append(results, protocol.NewBulkString(string(entry.Field())))
		results = append(results, protocol.NewBulkString(string(entry.Value())))
	}

	return protocol.NewArray(results)
}

func cmdHKeys(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	keys := hash.Keys()
	results := make([]*protocol.RESPValue, len(keys))
	for i, k := range keys {
		results[i] = protocol.NewBulkString(string(k))
	}

	return protocol.NewArray(results)
}

func cmdHVals(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	vals := hash.Values()
	results := make([]*protocol.RESPValue, len(vals))
	for i, v := range vals {
		results[i] = protocol.NewBulkString(string(v))
	}

	return protocol.NewArray(results)
}

func cmdHIncrBy(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	field := args[1].ToString()
	increment, err := strconv.ParseInt(args[2].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	obj, err := ctx.Db.Get(key)
	var hash *structure.RedisHash
	if err != nil {
		// 创建新的 Hash
		hashObj := storage.NewHashObject()
		ctx.Db.Set(key, hashObj)
		hash, _ = hashObj.GetHash()
	} else {
		hash, err = obj.GetHash()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
	}

	newVal, err := hash.IncrBy([]byte(field), increment)
	if err != nil {
		return protocol.NewError("ERR hash value is not an integer")
	}

	return protocol.NewInteger(newVal)
}

// ========== 连接命令实现 ==========

func cmdPing(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) > 0 {
		// PING message - 返回 message
		return protocol.NewBulkString(args[0].ToString())
	}
	// PING - 返回 PONG
	return protocol.NewSimpleString("PONG")
}

func cmdQuit(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	// 关闭客户端连接
	ctx.Client.Close()
	return protocol.NewSimpleString("OK")
}

// ========== 服务器命令实现 ==========

func cmdInfo(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	section := ""
	if len(args) > 0 {
		section = args[0].ToString()
	}

	var info strings.Builder

	switch section {
	case "server", "":
		info.WriteString("# Server\n")
		info.WriteString("redis_version:7.0.0\n")
		info.WriteString("redis_mode:standalone\n")
		info.WriteString("os:darwin\n")
		info.WriteString("arch_bits:64\n")
		info.WriteString("multiplexing_api:epoll\n")
		info.WriteString("process_id:1\n")
		info.WriteString("tcp_port:6379\n")
		info.WriteString("uptime_in_seconds:0\n")
		info.WriteString("uptime_in_days:0\n")

	case "clients":
		info.WriteString("# Clients\n")
		ctx.Server.mu.RLock()
		clientCount := len(ctx.Server.clients)
		ctx.Server.mu.RUnlock()
		info.WriteString(fmt.Sprintf("connected_clients:%d\n", clientCount))

	case "memory":
		info.WriteString("# Memory\n")
		ctx.Server.memoryStats.Update()
		info.WriteString(fmt.Sprintf("used_memory:%d\n", ctx.Server.memoryStats.GetUsedMemory()))
		info.WriteString(fmt.Sprintf("used_memory_human:%s\n", ctx.Server.memoryStats.GetUsedMemoryHuman()))
		info.WriteString(fmt.Sprintf("used_memory_peak:%d\n", ctx.Server.memoryStats.GetUsedMemoryPeak()))
		info.WriteString(fmt.Sprintf("used_memory_peak_human:%s\n", ctx.Server.memoryStats.GetUsedMemoryHuman()))

	case "stats":
		info.WriteString("# Stats\n")
		ctx.Server.stats.mu.RLock()
		info.WriteString(fmt.Sprintf("total_connections_received:%d\n", ctx.Server.stats.TotalConnectionsReceived))
		info.WriteString(fmt.Sprintf("total_commands_processed:%d\n", ctx.Server.stats.TotalCommandsProcessed))
		info.WriteString(fmt.Sprintf("keyspace_hits:%d\n", ctx.Server.stats.KeyspaceHits))
		info.WriteString(fmt.Sprintf("keyspace_misses:%d\n", ctx.Server.stats.KeyspaceMisses))
		ctx.Server.stats.mu.RUnlock()

	case "keyspace":
		info.WriteString("# Keyspace\n")
		for i := 0; i < ctx.Server.GetRedisServer().GetDbNum(); i++ {
			db, _ := ctx.Server.GetRedisServer().GetDb(i)
			if db != nil {
				size := db.DBSize()
				expires := db.ExpiresCount()
				if size > 0 || expires > 0 {
					info.WriteString(fmt.Sprintf("db%d:keys=%d,expires=%d,avg_ttl=0\n", i, size, expires))
				}
			}
		}

	default:
		// 返回所有信息
		info.WriteString("# Server\n")
		info.WriteString("redis_version:7.0.0\n")
		info.WriteString("redis_mode:standalone\n")
		info.WriteString("tcp_port:6379\n")
		info.WriteString("\n# Clients\n")
		ctx.Server.mu.RLock()
		clientCount := len(ctx.Server.clients)
		ctx.Server.mu.RUnlock()
		info.WriteString(fmt.Sprintf("connected_clients:%d\n", clientCount))
		ctx.Server.stats.mu.RLock()
		info.WriteString(fmt.Sprintf("total_connections_received:%d\n", ctx.Server.stats.TotalConnectionsReceived))
		ctx.Server.stats.mu.RUnlock()
		info.WriteString("\n# Memory\n")
		info.WriteString("used_memory:0\n")
		info.WriteString("\n# Stats\n")
		ctx.Server.stats.mu.RLock()
		info.WriteString(fmt.Sprintf("total_commands_processed:%d\n", ctx.Server.stats.TotalCommandsProcessed))
		info.WriteString(fmt.Sprintf("keyspace_hits:%d\n", ctx.Server.stats.KeyspaceHits))
		info.WriteString(fmt.Sprintf("keyspace_misses:%d\n", ctx.Server.stats.KeyspaceMisses))
		ctx.Server.stats.mu.RUnlock()
		info.WriteString("\n# Keyspace\n")
		for i := 0; i < ctx.Server.GetRedisServer().GetDbNum(); i++ {
			db, _ := ctx.Server.GetRedisServer().GetDb(i)
			size := db.DBSize()
			if size > 0 {
				info.WriteString(fmt.Sprintf("db%d:keys=%d\n", i, size))
			}
		}
	}

	return protocol.NewBulkString(info.String())
}

func cmdConfig(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) == 0 {
		return protocol.NewError("ERR wrong number of arguments for 'config' command")
	}

	subcommand := args[0].ToString()

	switch subcommand {
	case "GET":
		if len(args) < 2 {
			return protocol.NewError("ERR wrong number of arguments for 'config|get' command")
		}
		// 简化实现：返回空数组
		return protocol.NewArray([]*protocol.RESPValue{})

	case "SET":
		if len(args) < 3 {
			return protocol.NewError("ERR wrong number of arguments for 'config|set' command")
		}
		// 简化实现：返回 OK
		return protocol.NewSimpleString("OK")

	default:
		return protocol.NewError("ERR unknown subcommand or wrong number of arguments for 'config'")
	}
}

// ========== 事务命令实现 ==========

func cmdMulti(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if ctx.Client.inMulti {
		return protocol.NewError("ERR MULTI calls can not be nested")
	}

	ctx.Client.inMulti = true
	ctx.Client.transaction = NewTransaction()

	return protocol.NewSimpleString("OK")
}

func cmdExec(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if !ctx.Client.inMulti {
		return protocol.NewError("ERR EXEC without MULTI")
	}

	ctx.Client.inMulti = false

	if ctx.Client.transaction == nil || len(ctx.Client.transaction.commands) == 0 {
		ctx.Client.transaction = nil
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	// 保存命令列表用于 AOF 写入
	commands := make([]*QueuedCommand, len(ctx.Client.transaction.commands))
	copy(commands, ctx.Client.transaction.commands)

	// 执行事务
	results := ctx.Client.transaction.Execute(ctx)

	// 如果 AOF 已启用，写入事务中的所有写命令
	if ctx.Server.aofWriter != nil {
		for _, queuedCmd := range commands {
			if len(queuedCmd.cmd.GetArray()) > 0 {
				cmdName := queuedCmd.cmd.GetArray()[0].ToString()
				cmdName = toUpper(cmdName)
				if ctx.Server.isWriteCommand(cmdName) {
					// 检查命令执行结果是否成功（简化：总是写入）
					if err := ctx.Server.aofWriter.Append(queuedCmd.cmd); err != nil {
						fmt.Printf("AOF write error in transaction: %v\n", err)
					}
				}
			}
		}
	}

	ctx.Client.transaction = nil

	return protocol.NewArray(results)
}

func cmdDiscard(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if !ctx.Client.inMulti {
		return protocol.NewError("ERR DISCARD without MULTI")
	}

	ctx.Client.inMulti = false
	if ctx.Client.transaction != nil {
		ctx.Client.transaction.Discard()
		ctx.Client.transaction = nil
	}

	return protocol.NewSimpleString("OK")
}

func cmdWatch(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if ctx.Client.inMulti {
		return protocol.NewError("ERR WATCH inside MULTI is not allowed")
	}

	if ctx.Client.transaction == nil {
		ctx.Client.transaction = NewTransaction()
	}

	for i := 0; i < len(args); i++ {
		key := args[i].ToString()
		ctx.Client.transaction.Watch(key)
	}

	return protocol.NewInteger(int64(len(args)))
}

// ========== 发布订阅命令实现 ==========

func cmdPublish(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	channel := args[0].ToString()
	message := args[1].ToString()

	count := ctx.Server.pubsub.Publish(channel, message)
	return protocol.NewInteger(int64(count))
}

func cmdSubscribe(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	for i := 0; i < len(args); i++ {
		channel := args[i].ToString()
		ctx.Server.pubsub.Subscribe(ctx.Client, channel)

		// 发送订阅确认
		resp := protocol.NewArray([]*protocol.RESPValue{
			protocol.NewBulkString("subscribe"),
			protocol.NewBulkString(channel),
			protocol.NewInteger(1), // 订阅数量
		})
		ctx.Client.writeResponse(resp)
	}

	// 不返回，保持连接打开
	return nil
}

func cmdUnsubscribe(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) == 0 {
		// 取消所有订阅
		channels := ctx.Server.pubsub.Channels()
		for _, channel := range channels {
			ctx.Server.pubsub.Unsubscribe(ctx.Client, channel)
		}
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	// 取消指定订阅
	for i := 0; i < len(args); i++ {
		channel := args[i].ToString()
		ctx.Server.pubsub.Unsubscribe(ctx.Client, channel)
	}

	return protocol.NewArray([]*protocol.RESPValue{})
}

func cmdPSubscribe(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	for i := 0; i < len(args); i++ {
		pattern := args[i].ToString()
		ctx.Server.pubsub.PSubscribe(ctx.Client, pattern)

		// 发送订阅确认
		resp := protocol.NewArray([]*protocol.RESPValue{
			protocol.NewBulkString("psubscribe"),
			protocol.NewBulkString(pattern),
			protocol.NewInteger(1),
		})
		ctx.Client.writeResponse(resp)
	}

	return nil
}

func cmdPUnsubscribe(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	// 简化实现
	return protocol.NewArray([]*protocol.RESPValue{})
}

func cmdPubsub(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) == 0 {
		return protocol.NewError("ERR wrong number of arguments for 'pubsub' command")
	}

	subcommand := args[0].ToString()

	switch subcommand {
	case "NUMSUB":
		if len(args) < 2 {
			return protocol.NewError("ERR wrong number of arguments for 'pubsub|numsub' command")
		}
		// 返回频道和订阅数
		result := make([]*protocol.RESPValue, 0)
		for i := 1; i < len(args); i++ {
			channel := args[i].ToString()
			count := ctx.Server.pubsub.NumSub(channel)
			result = append(result, protocol.NewBulkString(channel))
			result = append(result, protocol.NewInteger(int64(count)))
		}
		return protocol.NewArray(result)

	case "NUMPAT":
		count := ctx.Server.pubsub.NumPat()
		return protocol.NewInteger(int64(count))

	case "CHANNELS":
		channels := ctx.Server.pubsub.Channels()
		result := make([]*protocol.RESPValue, len(channels))
		for i, ch := range channels {
			result[i] = protocol.NewBulkString(ch)
		}
		return protocol.NewArray(result)

	default:
		return protocol.NewError("ERR unknown subcommand or wrong number of arguments for 'pubsub'")
	}
}

// ========== 阻塞命令实现 ==========

func cmdBLPop(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments for 'blpop' command")
	}

	timeout, err := strconv.ParseInt(args[len(args)-1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR invalid timeout")
	}

	if timeout < 0 {
		return protocol.NewError("ERR timeout is negative")
	}

	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = args[i].ToString()
	}

	// 先尝试非阻塞弹出
	for _, key := range keys {
		obj, err := ctx.Db.Get(key)
		if err != nil {
			continue
		}

		list, err := obj.GetList()
		if err != nil {
			continue
		}

		if list.Len() > 0 {
			val, _ := list.Pop(0) // HEAD

			// 记录到 AOF
			if ctx.Server.aofWriter != nil {
				cmd := protocol.NewArray([]*protocol.RESPValue{
					protocol.NewBulkString("LPOP"),
					protocol.NewBulkString(key),
				})
				ctx.Server.aofWriter.Append(cmd)
			}

			return protocol.NewArray([]*protocol.RESPValue{
				protocol.NewBulkString(key),
				protocol.NewBulkString(string(val)),
			})
		}
	}

	// 如果超时为0，立即返回
	if timeout == 0 {
		return protocol.NewNullBulkString()
	}

	// 阻塞等待
	bc := ctx.Server.blockingMgr.Wait(ctx.Client, keys, timeout)

	// 等待通知或超时
	select {
	case result := <-bc.notify:
		return result
	case <-time.After(time.Duration(timeout) * time.Second):
		return protocol.NewNullBulkString()
	}
}

func cmdBRPop(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments for 'brpop' command")
	}

	timeout, err := strconv.ParseInt(args[len(args)-1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR invalid timeout")
	}

	if timeout < 0 {
		return protocol.NewError("ERR timeout is negative")
	}

	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = args[i].ToString()
	}

	// 先尝试非阻塞弹出
	for _, key := range keys {
		obj, err := ctx.Db.Get(key)
		if err != nil {
			continue
		}

		list, err := obj.GetList()
		if err != nil {
			continue
		}

		if list.Len() > 0 {
			val, _ := list.Pop(1) // TAIL

			// 记录到 AOF
			if ctx.Server.aofWriter != nil {
				cmd := protocol.NewArray([]*protocol.RESPValue{
					protocol.NewBulkString("RPOP"),
					protocol.NewBulkString(key),
				})
				ctx.Server.aofWriter.Append(cmd)
			}

			return protocol.NewArray([]*protocol.RESPValue{
				protocol.NewBulkString(key),
				protocol.NewBulkString(string(val)),
			})
		}
	}

	// 如果超时为0，立即返回
	if timeout == 0 {
		return protocol.NewNullBulkString()
	}

	// 阻塞等待
	bc := ctx.Server.blockingMgr.Wait(ctx.Client, keys, timeout)

	// 等待通知或超时
	select {
	case result := <-bc.notify:
		return result
	case <-time.After(time.Duration(timeout) * time.Second):
		return protocol.NewNullBulkString()
	}
}

// ========== 持久化命令实现 ==========

func cmdSave(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	// 获取 RDB 文件路径（默认：dump.rdb）
	filename := "dump.rdb"
	if len(args) > 0 {
		filename = args[0].ToString()
	}

	// 创建 RDB 编码器
	encoder := persistence.NewRDBEncoder(nil)

	// 保存到文件
	err := encoder.Save(ctx.Server.GetRedisServer(), filename)
	if err != nil {
		return protocol.NewError("ERR " + err.Error())
	}

	return protocol.NewSimpleString("OK")
}

func cmdBGSave(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	// 在后台 goroutine 中执行保存
	go func() {
		filename := "dump.rdb"
		encoder := persistence.NewRDBEncoder(nil)
		encoder.Save(ctx.Server.GetRedisServer(), filename)
	}()

	return protocol.NewSimpleString("Background saving started")
}

// ========== 集群命令实现 ==========

func cmdCluster(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) == 0 {
		return protocol.NewError("ERR wrong number of arguments for 'cluster' command")
	}

	subcommand := args[0].ToString()

	switch subcommand {
	case "SLOTS":
		// 返回槽分配信息
		// 简化实现：返回空数组
		// 实际应该返回所有槽的分配信息
		return protocol.NewArray([]*protocol.RESPValue{})

	case "NODES":
		// 返回节点信息
		// 简化实现：返回空字符串
		// 实际应该返回所有节点的信息
		return protocol.NewBulkString("")

	case "MEET":
		// 节点握手
		if len(args) < 3 {
			return protocol.NewError("ERR wrong number of arguments for 'cluster|meet' command")
		}
		ip := args[1].ToString()
		port := args[2].ToString()
		_ = fmt.Sprintf("%s:%s", ip, port) // 保留用于后续实现

		// 发送 MEET 消息
		// 简化实现：直接返回 OK
		// 实际应该通过 communicator 发送 MEET 消息
		return protocol.NewSimpleString("OK")

	case "INFO":
		// 返回集群信息
		info := fmt.Sprintf("cluster_state:ok\ncluster_slots_assigned:16384\ncluster_slots_ok:16384\ncluster_known_nodes:1\n")
		return protocol.NewBulkString(info)

	default:
		return protocol.NewError("ERR unknown subcommand or wrong number of arguments for 'cluster'")
	}
}

// ========== AOF 命令实现 ==========

func cmdBGRewriteAOF(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if ctx.Server.aofWriter == nil {
		return protocol.NewError("ERR AOF is not enabled")
	}

	// 在后台 goroutine 中执行 AOF 重写
	go func() {
		ctx.Server.aofWriter.Rewrite(ctx.Server.GetRedisServer())
	}()

	return protocol.NewSimpleString("Background append only file rewriting started")
}

// ========== 阻塞 ZSet 命令实现 ==========

func cmdBZPopMax(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments for 'bzpopmax' command")
	}

	timeout, err := strconv.ParseInt(args[len(args)-1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR invalid timeout")
	}

	if timeout < 0 {
		return protocol.NewError("ERR timeout is negative")
	}

	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = args[i].ToString()
	}

	// 先尝试非阻塞弹出
	for _, key := range keys {
		obj, err := ctx.Db.Get(key)
		if err != nil {
			continue
		}

		zset, err := obj.GetZSet()
		if err != nil {
			continue
		}

		if zset.Card() > 0 {
			// 获取最大元素
			entries, _ := zset.Range(0, 0, true) // 降序，第一个
			if len(entries) > 0 {
				entry := entries[0]
				zset.Remove(entry.Member())

				// 记录到 AOF
				if ctx.Server.aofWriter != nil {
					cmd := protocol.NewArray([]*protocol.RESPValue{
						protocol.NewBulkString("ZREM"),
						protocol.NewBulkString(key),
						protocol.NewBulkString(string(entry.Member())),
					})
					ctx.Server.aofWriter.Append(cmd)
				}

				return protocol.NewArray([]*protocol.RESPValue{
					protocol.NewBulkString(key),
					protocol.NewBulkString(string(entry.Member())),
					protocol.NewBulkString(strconv.FormatFloat(entry.Score(), 'f', -1, 64)),
				})
			}
		}
	}

	// 如果超时为0，立即返回
	if timeout == 0 {
		return protocol.NewNullBulkString()
	}

	// 阻塞等待（简化实现：不实现真正的阻塞）
	return protocol.NewNullBulkString()
}

func cmdBZPopMin(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments for 'bzpopmin' command")
	}

	timeout, err := strconv.ParseInt(args[len(args)-1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR invalid timeout")
	}

	if timeout < 0 {
		return protocol.NewError("ERR timeout is negative")
	}

	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = args[i].ToString()
	}

	// 先尝试非阻塞弹出
	for _, key := range keys {
		obj, err := ctx.Db.Get(key)
		if err != nil {
			continue
		}

		zset, err := obj.GetZSet()
		if err != nil {
			continue
		}

		if zset.Card() > 0 {
			// 获取最小元素
			entries, _ := zset.Range(0, 0, false) // 升序，第一个
			if len(entries) > 0 {
				entry := entries[0]
				zset.Remove(entry.Member())

				// 记录到 AOF
				if ctx.Server.aofWriter != nil {
					cmd := protocol.NewArray([]*protocol.RESPValue{
						protocol.NewBulkString("ZREM"),
						protocol.NewBulkString(key),
						protocol.NewBulkString(string(entry.Member())),
					})
					ctx.Server.aofWriter.Append(cmd)
				}

				return protocol.NewArray([]*protocol.RESPValue{
					protocol.NewBulkString(key),
					protocol.NewBulkString(string(entry.Member())),
					protocol.NewBulkString(strconv.FormatFloat(entry.Score(), 'f', -1, 64)),
				})
			}
		}
	}

	// 如果超时为0，立即返回
	if timeout == 0 {
		return protocol.NewNullBulkString()
	}

	// 阻塞等待（简化实现：不实现真正的阻塞）
	return protocol.NewNullBulkString()
}
