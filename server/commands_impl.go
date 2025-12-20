package server

import (
	"fmt"
	"github.com/code-100-precent/LingCache/persistence"
	"github.com/code-100-precent/LingCache/protocol"
	"github.com/code-100-precent/LingCache/replication"
	"github.com/code-100-precent/LingCache/storage"
	"github.com/code-100-precent/LingCache/structure"
	"sort"
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

func cmdSetEx(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	seconds, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}
	value := args[2].ToString()

	obj := storage.NewStringObject([]byte(value))
	ctx.Db.Set(key, obj)
	ctx.Db.Expire(key, seconds)

	return protocol.NewSimpleString("OK")
}

func cmdSetNx(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	value := args[1].ToString()

	// 检查键是否存在
	if ctx.Db.Exists(key) {
		return protocol.NewInteger(0)
	}

	obj := storage.NewStringObject([]byte(value))
	ctx.Db.Set(key, obj)

	return protocol.NewInteger(1)
}

func cmdPSetEx(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	milliseconds, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}
	value := args[2].ToString()

	obj := storage.NewStringObject([]byte(value))
	ctx.Db.Set(key, obj)
	// 使用秒级过期（简化实现，实际应该支持毫秒级）
	ctx.Db.Expire(key, milliseconds/1000)

	return protocol.NewSimpleString("OK")
}

func cmdGetSet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	newValue := args[1].ToString()

	// 获取旧值
	oldObj, err := ctx.Db.Get(key)
	var oldValue string
	if err != nil {
		oldValue = ""
	} else {
		val, err := oldObj.GetStringValue()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		oldValue = string(val)
	}

	// 设置新值
	obj := storage.NewStringObject([]byte(newValue))
	ctx.Db.Set(key, obj)

	if oldValue == "" {
		return protocol.NewNullBulkString()
	}
	return protocol.NewBulkString(oldValue)
}

func cmdAppend(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	appendValue := args[1].ToString()

	obj, err := ctx.Db.Get(key)
	var currentValue string
	if err != nil {
		// 键不存在，创建新字符串
		currentValue = ""
	} else {
		val, err := obj.GetStringValue()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		currentValue = string(val)
	}

	// 追加值
	newValue := currentValue + appendValue
	newObj := storage.NewStringObject([]byte(newValue))
	ctx.Db.Set(key, newObj)

	return protocol.NewInteger(int64(len(newValue)))
}

func cmdStrLen(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	val, err := obj.GetStringValue()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	return protocol.NewInteger(int64(len(val)))
}

func cmdIncr(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	return cmdIncrBy(ctx, []*protocol.RESPValue{args[0], protocol.NewInteger(1)})
}

func cmdDecr(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	return cmdIncrBy(ctx, []*protocol.RESPValue{args[0], protocol.NewInteger(-1)})
}

func cmdIncrBy(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	increment, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	obj, err := ctx.Db.Get(key)
	var currentValue int64
	if err != nil {
		// 键不存在，从 0 开始
		currentValue = 0
	} else {
		val, err := obj.GetStringValue()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		// 尝试解析为整数
		parsed, err := strconv.ParseInt(string(val), 10, 64)
		if err != nil {
			return protocol.NewError("ERR value is not an integer or out of range")
		}
		currentValue = parsed
	}

	// 计算新值
	newValue := currentValue + increment
	newObj := storage.NewStringObject([]byte(strconv.FormatInt(newValue, 10)))
	ctx.Db.Set(key, newObj)

	return protocol.NewInteger(newValue)
}

func cmdDecrBy(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	decrement, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	// 使用 INCRBY 的负数实现
	return cmdIncrBy(ctx, []*protocol.RESPValue{args[0], protocol.NewInteger(-decrement)})
}

func cmdGetRange(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	start, err := strconv.Atoi(args[1].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewBulkString("")
	}

	val, err := obj.GetStringValue()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	str := string(val)
	length := len(str)

	// 处理负数索引
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// 边界检查
	if start < 0 {
		start = 0
	}
	if start >= length {
		return protocol.NewBulkString("")
	}
	if end >= length {
		end = length - 1
	}
	if end < start {
		return protocol.NewBulkString("")
	}

	return protocol.NewBulkString(str[start : end+1])
}

func cmdSetRange(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	offset, err := strconv.Atoi(args[1].ToString())
	if err != nil || offset < 0 {
		return protocol.NewError("ERR value is not an integer or out of range")
	}
	value := args[2].ToString()

	obj, err := ctx.Db.Get(key)
	var currentValue string
	if err != nil {
		// 键不存在，创建空字符串
		currentValue = ""
	} else {
		val, err := obj.GetStringValue()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		currentValue = string(val)
	}

	// 扩展字符串到需要的长度
	requiredLength := offset + len(value)
	if requiredLength > len(currentValue) {
		// 用空字符填充
		padding := make([]byte, requiredLength-len(currentValue))
		currentValue = currentValue + string(padding)
	}

	// 替换指定范围
	result := []byte(currentValue)
	copy(result[offset:], []byte(value))
	newObj := storage.NewStringObject(result)
	ctx.Db.Set(key, newObj)

	return protocol.NewInteger(int64(len(result)))
}

func cmdSetBit(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	offset, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil || offset < 0 {
		return protocol.NewError("ERR bit offset is not an integer or out of range")
	}
	bit, err := strconv.Atoi(args[2].ToString())
	if err != nil || (bit != 0 && bit != 1) {
		return protocol.NewError("ERR bit is not an integer or out of range")
	}

	obj, err := ctx.Db.Get(key)
	var currentValue []byte
	if err != nil {
		// 键不存在，创建空字符串
		currentValue = []byte{}
	} else {
		val, err := obj.GetStringValue()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		currentValue = val
	}

	// 计算需要的字节数
	byteIndex := int(offset / 8)
	bitIndex := int(offset % 8)

	// 扩展字符串到需要的长度
	if byteIndex >= len(currentValue) {
		// 扩展字符串
		extension := make([]byte, byteIndex+1-len(currentValue))
		currentValue = append(currentValue, extension...)
	}

	// 获取旧位的值
	oldByte := currentValue[byteIndex]
	oldBit := (oldByte >> (7 - bitIndex)) & 1

	// 设置新位
	if bit == 1 {
		currentValue[byteIndex] |= (1 << (7 - bitIndex))
	} else {
		currentValue[byteIndex] &= ^(1 << (7 - bitIndex))
	}

	// 保存
	newObj := storage.NewStringObject(currentValue)
	ctx.Db.Set(key, newObj)

	return protocol.NewInteger(int64(oldBit))
}

func cmdGetBit(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	offset, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil || offset < 0 {
		return protocol.NewError("ERR bit offset is not an integer or out of range")
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	val, err := obj.GetStringValue()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 计算字节和位索引
	byteIndex := int(offset / 8)
	bitIndex := int(offset % 8)

	if byteIndex >= len(val) {
		return protocol.NewInteger(0)
	}

	// 获取位的值
	bit := (val[byteIndex] >> (7 - bitIndex)) & 1
	return protocol.NewInteger(int64(bit))
}

func cmdBitCount(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	start := 0
	end := -1

	if len(args) > 1 {
		var err error
		start, err = strconv.Atoi(args[1].ToString())
		if err != nil {
			return protocol.NewError("ERR value is not an integer or out of range")
		}
	}
	if len(args) > 2 {
		var err error
		end, err = strconv.Atoi(args[2].ToString())
		if err != nil {
			return protocol.NewError("ERR value is not an integer or out of range")
		}
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	val, err := obj.GetStringValue()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 处理负数索引
	length := len(val)
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// 边界检查
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return protocol.NewInteger(0)
	}

	// 计算范围内的位计数
	count := 0
	for i := start; i <= end; i++ {
		byteVal := val[i]
		// 计算字节中1的个数
		for j := 0; j < 8; j++ {
			if (byteVal>>(7-j))&1 == 1 {
				count++
			}
		}
	}

	return protocol.NewInteger(int64(count))
}

func cmdBitOp(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	operation := strings.ToUpper(args[0].ToString())
	destKey := args[1].ToString()

	if operation != "AND" && operation != "OR" && operation != "XOR" && operation != "NOT" {
		return protocol.NewError("ERR syntax error")
	}

	// 获取源键
	sources := make([][]byte, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		key := args[i].ToString()
		obj, err := ctx.Db.Get(key)
		if err != nil {
			if operation == "NOT" {
				return protocol.NewError("ERR no such key")
			}
			// 对于其他操作，不存在的键视为空字符串
			sources = append(sources, []byte{})
			continue
		}
		val, err := obj.GetStringValue()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		sources = append(sources, val)
	}

	if len(sources) == 0 {
		return protocol.NewError("ERR wrong number of arguments")
	}

	// 执行位操作
	var result []byte
	if operation == "NOT" {
		if len(sources) != 1 {
			return protocol.NewError("ERR wrong number of arguments")
		}
		result = make([]byte, len(sources[0]))
		for i, b := range sources[0] {
			result[i] = ^b
		}
	} else {
		// 找到最大长度
		maxLen := 0
		for _, src := range sources {
			if len(src) > maxLen {
				maxLen = len(src)
			}
		}

		result = make([]byte, maxLen)
		for i := 0; i < maxLen; i++ {
			var byteVal byte
			if operation == "AND" {
				byteVal = 0xFF
				for _, src := range sources {
					if i < len(src) {
						byteVal &= src[i]
					} else {
						byteVal = 0
						break
					}
				}
			} else if operation == "OR" {
				for _, src := range sources {
					if i < len(src) {
						byteVal |= src[i]
					}
				}
			} else if operation == "XOR" {
				for _, src := range sources {
					if i < len(src) {
						byteVal ^= src[i]
					}
				}
			}
			result[i] = byteVal
		}
	}

	// 保存结果
	resultObj := storage.NewStringObject(result)
	ctx.Db.Set(destKey, resultObj)

	return protocol.NewInteger(int64(len(result)))
}

func cmdBitPos(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	bit, err := strconv.Atoi(args[1].ToString())
	if err != nil || (bit != 0 && bit != 1) {
		return protocol.NewError("ERR bit is not an integer or out of range")
	}

	start := 0
	end := -1
	if len(args) > 2 {
		start, err = strconv.Atoi(args[2].ToString())
		if err != nil {
			return protocol.NewError("ERR value is not an integer or out of range")
		}
	}
	if len(args) > 3 {
		end, err = strconv.Atoi(args[3].ToString())
		if err != nil {
			return protocol.NewError("ERR value is not an integer or out of range")
		}
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(-1)
	}

	val, err := obj.GetStringValue()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 处理负数索引
	length := len(val)
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// 边界检查
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return protocol.NewInteger(-1)
	}

	// 查找第一个指定位的位位置
	for i := start; i <= end; i++ {
		byteVal := val[i]
		for j := 0; j < 8; j++ {
			bitVal := (byteVal >> (7 - j)) & 1
			if int(bitVal) == bit {
				offset := int64(i*8 + j)
				return protocol.NewInteger(offset)
			}
		}
	}

	return protocol.NewInteger(-1)
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

func cmdExpireAt(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	timestamp, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	if ctx.Db.ExpireAt(key, timestamp) {
		return protocol.NewInteger(1)
	}
	return protocol.NewInteger(0)
}

func cmdPExpire(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	milliseconds, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	// 转换为秒（简化实现，实际应该支持毫秒级）
	seconds := milliseconds / 1000
	if ctx.Db.Expire(key, seconds) {
		return protocol.NewInteger(1)
	}
	return protocol.NewInteger(0)
}

func cmdPExpireAt(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	milliseconds, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	// 转换为秒级时间戳（简化实现）
	timestamp := milliseconds / 1000
	if ctx.Db.ExpireAt(key, timestamp) {
		return protocol.NewInteger(1)
	}
	return protocol.NewInteger(0)
}

func cmdPTTL(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	ttl, err := ctx.Db.TTL(key)
	if err != nil {
		return protocol.NewInteger(-2) // 键不存在
	}

	if ttl < 0 {
		return protocol.NewInteger(ttl)
	}

	// 转换为毫秒
	return protocol.NewInteger(ttl * 1000)
}

func cmdPersist(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	if ctx.Db.Persist(key) {
		return protocol.NewInteger(1)
	}
	return protocol.NewInteger(0)
}

func cmdRename(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	newKey := args[1].ToString()

	if key == newKey {
		return protocol.NewError("ERR source and destination objects are the same")
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewError("ERR no such key")
	}

	// 删除旧键
	ctx.Db.Del(key)

	// 如果新键存在，先删除
	ctx.Db.Del(newKey)

	// 设置新键
	ctx.Db.Set(newKey, obj)

	return protocol.NewSimpleString("OK")
}

func cmdRenameNx(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	newKey := args[1].ToString()

	if key == newKey {
		return protocol.NewError("ERR source and destination objects are the same")
	}

	// 检查新键是否存在
	if ctx.Db.Exists(newKey) {
		return protocol.NewInteger(0)
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewError("ERR no such key")
	}

	// 删除旧键
	ctx.Db.Del(key)

	// 设置新键
	ctx.Db.Set(newKey, obj)

	return protocol.NewInteger(1)
}

func cmdRandomKey(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	keys := ctx.Db.Keys("*")
	if len(keys) == 0 {
		return protocol.NewNullBulkString()
	}

	// 简化实现：返回第一个键（实际应该随机）
	return protocol.NewBulkString(keys[0])
}

func cmdMove(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	dbIndex, err := strconv.Atoi(args[1].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	// 验证数据库索引
	redisServer := ctx.Server.GetRedisServer()
	if dbIndex < 0 || dbIndex >= redisServer.GetDbNum() {
		return protocol.NewError("ERR DB index is out of range")
	}

	// 检查键是否存在
	if !ctx.Db.Exists(key) {
		return protocol.NewInteger(0)
	}

	// 检查目标数据库是否已有该键
	targetDb, err := redisServer.GetDb(dbIndex)
	if err != nil {
		return protocol.NewError("ERR invalid DB index")
	}

	if targetDb.Exists(key) {
		return protocol.NewInteger(0)
	}

	// 获取对象
	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	// 从当前数据库删除
	ctx.Db.Del(key)

	// 添加到目标数据库
	targetDb.Set(key, obj)

	return protocol.NewInteger(1)
}

func cmdObject(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments")
	}

	subcommand := strings.ToUpper(args[0].ToString())
	key := args[1].ToString()

	switch subcommand {
	case "ENCODING":
		obj, err := ctx.Db.Get(key)
		if err != nil {
			return protocol.NewError("ERR no such key")
		}
		return protocol.NewBulkString(obj.EncodingString())
	case "REFCOUNT":
		obj, err := ctx.Db.Get(key)
		if err != nil {
			return protocol.NewError("ERR no such key")
		}
		return protocol.NewInteger(int64(obj.RefCount))
	case "IDLETIME":
		// 简化实现：返回 0
		return protocol.NewInteger(0)
	default:
		return protocol.NewError("ERR unknown subcommand or wrong number of arguments")
	}
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

func cmdLIndex(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	index, err := strconv.Atoi(args[1].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 使用 Range 获取单个元素
	values, err := list.Range(index, index)
	if err != nil || len(values) == 0 {
		return protocol.NewNullBulkString()
	}

	return protocol.NewBulkString(string(values[0]))
}

func cmdLInsert(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	where := strings.ToUpper(args[1].ToString())
	pivot := args[2].ToString()
	value := args[3].ToString()

	if where != "BEFORE" && where != "AFTER" {
		return protocol.NewError("ERR syntax error")
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 查找 pivot 的位置
	length := list.Len()
	found := false
	pivotIndex := -1

	for i := 0; i < length; i++ {
		values, _ := list.Range(i, i)
		if len(values) > 0 && string(values[0]) == pivot {
			pivotIndex = i
			found = true
			break
		}
	}

	if !found {
		return protocol.NewInteger(-1)
	}

	// 插入位置
	insertIndex := pivotIndex
	if where == "AFTER" {
		insertIndex = pivotIndex + 1
	}

	// 获取所有元素
	allValues, _ := list.Range(0, length-1)

	// 重建列表
	ctx.Db.Del(key)
	newListObj := storage.NewListObject()
	newList, _ := newListObj.GetList()

	// 插入新元素
	for i := 0; i < len(allValues); i++ {
		if i == insertIndex {
			newList.Push([]byte(value), 1) // TAIL
		}
		newList.Push(allValues[i], 1) // TAIL
	}
	if insertIndex >= len(allValues) {
		newList.Push([]byte(value), 1) // TAIL
	}

	ctx.Db.Set(key, newListObj)

	return protocol.NewInteger(int64(list.Len() + 1))
}

func cmdLRem(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	count, err := strconv.Atoi(args[1].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}
	value := args[2].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	length := list.Len()
	if length == 0 {
		return protocol.NewInteger(0)
	}

	// 获取所有元素
	allValues, _ := list.Range(0, length-1)

	// 重建列表，移除匹配的元素
	removed := 0
	newValues := make([][]byte, 0, length)

	if count == 0 {
		// 移除所有匹配的元素
		for _, v := range allValues {
			if string(v) != value {
				newValues = append(newValues, v)
			} else {
				removed++
			}
		}
	} else if count > 0 {
		// 从头部开始移除 count 个
		removedCount := 0
		for _, v := range allValues {
			if string(v) == value && removedCount < count {
				removedCount++
				removed++
			} else {
				newValues = append(newValues, v)
			}
		}
	} else {
		// 从尾部开始移除 |count| 个
		removedCount := 0
		for i := len(allValues) - 1; i >= 0; i-- {
			v := allValues[i]
			if string(v) == value && removedCount < -count {
				removedCount++
				removed++
			} else {
				// 在头部插入
				newValues = append([][]byte{v}, newValues...)
			}
		}
	}

	// 重建列表
	ctx.Db.Del(key)
	if len(newValues) > 0 {
		newListObj := storage.NewListObject()
		newList, _ := newListObj.GetList()
		for _, v := range newValues {
			newList.Push(v, 1) // TAIL
		}
		ctx.Db.Set(key, newListObj)
	}

	return protocol.NewInteger(int64(removed))
}

func cmdLSet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	index, err := strconv.Atoi(args[1].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}
	value := args[2].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewError("ERR no such key")
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	length := list.Len()

	// 处理负数索引
	if index < 0 {
		index = length + index
	}

	if index < 0 || index >= length {
		return protocol.NewError("ERR index out of range")
	}

	// 获取所有元素
	allValues, _ := list.Range(0, length-1)

	// 修改指定索引的值
	allValues[index] = []byte(value)

	// 重建列表
	ctx.Db.Del(key)
	newListObj := storage.NewListObject()
	newList, _ := newListObj.GetList()
	for _, v := range allValues {
		newList.Push(v, 1) // TAIL
	}
	ctx.Db.Set(key, newListObj)

	return protocol.NewSimpleString("OK")
}

func cmdLTrim(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	start, err := strconv.Atoi(args[1].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewSimpleString("OK")
	}

	list, err := obj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	length := list.Len()

	// 处理负数索引
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// 边界检查
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		// 清空列表
		ctx.Db.Del(key)
		return protocol.NewSimpleString("OK")
	}

	// 获取范围内的元素
	values, _ := list.Range(start, end)

	// 重建列表
	ctx.Db.Del(key)
	if len(values) > 0 {
		newListObj := storage.NewListObject()
		newList, _ := newListObj.GetList()
		for _, v := range values {
			newList.Push(v, 1) // TAIL
		}
		ctx.Db.Set(key, newListObj)
	}

	return protocol.NewSimpleString("OK")
}

func cmdRPopLPush(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	source := args[0].ToString()
	destination := args[1].ToString()

	// 从源列表弹出
	sourceObj, err := ctx.Db.Get(source)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	sourceList, err := sourceObj.GetList()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	if sourceList.Len() == 0 {
		return protocol.NewNullBulkString()
	}

	// 弹出尾部元素
	value, err := sourceList.Pop(1) // TAIL
	if err != nil {
		return protocol.NewNullBulkString()
	}

	// 更新源列表
	if sourceList.Len() == 0 {
		ctx.Db.Del(source)
	} else {
		// 重建源列表（简化实现）
		allValues, _ := sourceList.Range(0, sourceList.Len()-1)
		ctx.Db.Del(source)
		if len(allValues) > 0 {
			newSourceObj := storage.NewListObject()
			newSourceList, _ := newSourceObj.GetList()
			for _, v := range allValues {
				newSourceList.Push(v, 1)
			}
			ctx.Db.Set(source, newSourceObj)
		}
	}

	// 推入目标列表头部
	destObj, err := ctx.Db.Get(destination)
	var destList *structure.RedisList
	if err != nil {
		// 创建新列表
		destObj = storage.NewListObject()
		ctx.Db.Set(destination, destObj)
		destList, _ = destObj.GetList()
	} else {
		destList, err = destObj.GetList()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
	}

	destList.Push(value, 0) // HEAD

	return protocol.NewBulkString(string(value))
}

func cmdBRPopLPush(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	source := args[0].ToString()
	_ = args[1].ToString() // destination
	_, err := strconv.Atoi(args[2].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	// 先尝试非阻塞操作
	sourceObj, err := ctx.Db.Get(source)
	if err == nil {
		sourceList, err := sourceObj.GetList()
		if err == nil && sourceList.Len() > 0 {
			// 有数据，直接执行 RPOPLPUSH
			return cmdRPopLPush(ctx, args[:2])
		}
	}

	// 没有数据，进入阻塞模式（简化实现：立即返回 nil）
	// 实际应该实现阻塞逻辑
	return protocol.NewNullBulkString()
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

func cmdSPop(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1].ToString())
		if err != nil || count < 0 {
			return protocol.NewError("ERR value is not an integer or out of range")
		}
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	set, err := obj.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	if set.Card() == 0 {
		return protocol.NewNullBulkString()
	}

	// 获取随机成员并删除
	members := set.Members()
	if count >= len(members) {
		count = len(members)
	}

	// 简化实现：随机选择（实际应该使用真正的随机）
	popped := make([][]byte, 0, count)
	for i := 0; i < count && len(members) > 0; i++ {
		member := members[i]
		set.Remove(member)
		popped = append(popped, member)
		// 更新 members（移除已弹出的）
		members = append(members[:i], members[i+1:]...)
		i--
	}

	if len(popped) == 0 {
		return protocol.NewNullBulkString()
	}
	if len(popped) == 1 {
		return protocol.NewBulkString(string(popped[0]))
	}

	// 返回数组
	results := make([]*protocol.RESPValue, len(popped))
	for i, m := range popped {
		results[i] = protocol.NewBulkString(string(m))
	}
	return protocol.NewArray(results)
}

func cmdSRandMember(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(args[1].ToString())
		if err != nil {
			return protocol.NewError("ERR value is not an integer or out of range")
		}
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewNullBulkString()
	}

	set, err := obj.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	if set.Card() == 0 {
		return protocol.NewNullBulkString()
	}

	if count == 0 {
		count = 1
	}

	// 简化实现：返回第一个成员（实际应该随机）
	if count == 1 {
		member := set.RandomMember()
		if member == nil {
			return protocol.NewNullBulkString()
		}
		return protocol.NewBulkString(string(member))
	}

	// 返回多个（可能重复）
	results := make([]*protocol.RESPValue, 0, count)
	for i := 0; i < count; i++ {
		member := set.RandomMember()
		if member != nil {
			results = append(results, protocol.NewBulkString(string(member)))
		}
	}

	return protocol.NewArray(results)
}

func cmdSMove(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	source := args[0].ToString()
	destination := args[1].ToString()
	member := args[2].ToString()

	// 从源集合获取
	sourceObj, err := ctx.Db.Get(source)
	if err != nil {
		return protocol.NewInteger(0)
	}

	sourceSet, err := sourceObj.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 检查成员是否存在
	if !sourceSet.IsMember([]byte(member)) {
		return protocol.NewInteger(0)
	}

	// 从源集合删除
	sourceSet.Remove([]byte(member))

	// 添加到目标集合
	destObj, err := ctx.Db.Get(destination)
	var destSet *structure.RedisSet
	if err != nil {
		// 创建新集合
		destObj = storage.NewSetObject()
		ctx.Db.Set(destination, destObj)
		destSet, _ = destObj.GetSet()
	} else {
		destSet, err = destObj.GetSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
	}

	destSet.Add([]byte(member))

	return protocol.NewInteger(1)
}

func cmdSInterStore(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	destination := args[0].ToString()

	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments")
	}

	// 获取所有源集合
	sets := make([]*structure.RedisSet, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		key := args[i].ToString()
		obj, err := ctx.Db.Get(key)
		if err != nil {
			// 如果任何一个集合不存在，结果为空
			ctx.Db.Del(destination)
			return protocol.NewInteger(0)
		}
		set, err := obj.GetSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		sets = append(sets, set)
	}

	if len(sets) == 0 {
		ctx.Db.Del(destination)
		return protocol.NewInteger(0)
	}

	// 计算交集
	result := sets[0]
	if len(sets) > 1 {
		result = result.Inter(sets[1:]...)
	}

	// 保存结果
	resultObj := storage.NewSetObject()
	resultSet, _ := resultObj.GetSet()
	members := result.Members()
	for _, member := range members {
		resultSet.Add(member)
	}
	ctx.Db.Set(destination, resultObj)

	return protocol.NewInteger(int64(len(members)))
}

func cmdSUnionStore(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	destination := args[0].ToString()

	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments")
	}

	// 获取所有源集合
	sets := make([]*structure.RedisSet, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		key := args[i].ToString()
		obj, err := ctx.Db.Get(key)
		if err != nil {
			continue // 跳过不存在的集合
		}
		set, err := obj.GetSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		sets = append(sets, set)
	}

	if len(sets) == 0 {
		ctx.Db.Del(destination)
		return protocol.NewInteger(0)
	}

	// 计算并集
	result := sets[0]
	if len(sets) > 1 {
		result = result.Union(sets[1:]...)
	}

	// 保存结果
	resultObj := storage.NewSetObject()
	resultSet, _ := resultObj.GetSet()
	members := result.Members()
	for _, member := range members {
		resultSet.Add(member)
	}
	ctx.Db.Set(destination, resultObj)

	return protocol.NewInteger(int64(len(members)))
}

func cmdSDiffStore(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	destination := args[0].ToString()

	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments")
	}

	// 获取第一个集合
	key1 := args[1].ToString()
	obj1, err := ctx.Db.Get(key1)
	if err != nil {
		ctx.Db.Del(destination)
		return protocol.NewInteger(0)
	}

	set1, err := obj1.GetSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 获取其他集合
	others := make([]*structure.RedisSet, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		key := args[i].ToString()
		obj, err := ctx.Db.Get(key)
		if err != nil {
			continue // 跳过不存在的集合
		}
		set, err := obj.GetSet()
		if err != nil {
			return protocol.NewError("ERR wrong type")
		}
		others = append(others, set)
	}

	// 计算差集
	result := set1
	if len(others) > 0 {
		result = result.Diff(others...)
	}

	// 保存结果
	resultObj := storage.NewSetObject()
	resultSet, _ := resultObj.GetSet()
	members := result.Members()
	for _, member := range members {
		resultSet.Add(member)
	}
	ctx.Db.Set(destination, resultObj)

	return protocol.NewInteger(int64(len(members)))
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

func cmdZRevRange(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	start, _ := strconv.Atoi(args[1].ToString())
	end, _ := strconv.Atoi(args[2].ToString())
	withScores := false
	if len(args) > 3 && strings.ToUpper(args[3].ToString()) == "WITHSCORES" {
		withScores = true
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 获取所有元素（按 score 排序）
	entries, _ := zset.Range(0, -1, false)
	length := len(entries)

	// 处理负数索引
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// 边界检查
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	// 反转范围（从尾部开始）
	revStart := length - 1 - end
	revEnd := length - 1 - start

	// 获取反转后的元素
	results := make([]*protocol.RESPValue, 0)
	for i := revStart; i <= revEnd; i++ {
		if i >= 0 && i < length {
			entry := entries[i]
			results = append(results, protocol.NewBulkString(string(entry.Member())))
			if withScores {
				results = append(results, protocol.NewBulkString(strconv.FormatFloat(entry.Score(), 'f', -1, 64)))
			}
		}
	}

	return protocol.NewArray(results)
}

func cmdZRevRank(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
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

	// 获取正向排名
	rank, exists := zset.Rank([]byte(member), false)
	if !exists {
		return protocol.NewNullBulkString()
	}

	// 计算反向排名
	card := zset.Card()
	revRank := card - 1 - rank

	return protocol.NewInteger(int64(revRank))
}

func cmdZIncrBy(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	increment, err := strconv.ParseFloat(args[1].ToString(), 64)
	if err != nil {
		return protocol.NewError("ERR value is not a valid float")
	}
	member := args[2].ToString()

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

	// 获取当前 score
	currentScore, _ := zset.Score([]byte(member))
	newScore := currentScore + increment

	// 更新或添加
	zset.Add([]byte(member), newScore)

	return protocol.NewBulkString(strconv.FormatFloat(newScore, 'f', -1, 64))
}

func cmdZRangeByScore(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	min := args[1].ToString()
	max := args[2].ToString()
	withScores := false
	offset := 0
	count := -1

	// 解析可选参数
	for i := 3; i < len(args); i++ {
		arg := strings.ToUpper(args[i].ToString())
		if arg == "WITHSCORES" {
			withScores = true
		} else if arg == "LIMIT" && i+2 < len(args) {
			offset, _ = strconv.Atoi(args[i+1].ToString())
			count, _ = strconv.Atoi(args[i+2].ToString())
			i += 2
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

	// 解析分数范围
	minScore, minInclusive := parseScore(min)
	maxScore, maxInclusive := parseScore(max)

	// 获取所有元素
	entries, _ := zset.Range(0, -1, false)
	results := make([]*protocol.RESPValue, 0)

	for _, entry := range entries {
		score := entry.Score()
		if (minInclusive && score >= minScore || !minInclusive && score > minScore) &&
			(maxInclusive && score <= maxScore || !maxInclusive && score < maxScore) {
			if offset > 0 {
				offset--
				continue
			}
			if count == 0 {
				break
			}
			results = append(results, protocol.NewBulkString(string(entry.Member())))
			if withScores {
				results = append(results, protocol.NewBulkString(strconv.FormatFloat(score, 'f', -1, 64)))
			}
			if count > 0 {
				count--
			}
		}
	}

	return protocol.NewArray(results)
}

func cmdZCount(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	min := args[1].ToString()
	max := args[2].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 解析分数范围
	minScore, minInclusive := parseScore(min)
	maxScore, maxInclusive := parseScore(max)

	// 获取所有元素并计数
	entries, _ := zset.Range(0, -1, false)
	count := 0

	for _, entry := range entries {
		score := entry.Score()
		if (minInclusive && score >= minScore || !minInclusive && score > minScore) &&
			(maxInclusive && score <= maxScore || !maxInclusive && score < maxScore) {
			count++
		}
	}

	return protocol.NewInteger(int64(count))
}

// parseScore 解析分数字符串（支持 (min, [min, -inf, +inf）
func parseScore(scoreStr string) (float64, bool) {
	inclusive := true
	if strings.HasPrefix(scoreStr, "(") {
		inclusive = false
		scoreStr = scoreStr[1:]
	}

	if scoreStr == "-inf" {
		return -1e308, inclusive
	}
	if scoreStr == "+inf" || scoreStr == "inf" {
		return 1e308, inclusive
	}

	score, err := strconv.ParseFloat(scoreStr, 64)
	if err != nil {
		return 0, true
	}
	return score, inclusive
}

func cmdZRevRangeByScore(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	max := args[1].ToString()
	min := args[2].ToString()
	withScores := false
	offset := 0
	count := -1

	// 解析可选参数
	for i := 3; i < len(args); i++ {
		arg := strings.ToUpper(args[i].ToString())
		if arg == "WITHSCORES" {
			withScores = true
		} else if arg == "LIMIT" && i+2 < len(args) {
			offset, _ = strconv.Atoi(args[i+1].ToString())
			count, _ = strconv.Atoi(args[i+2].ToString())
			i += 2
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

	// 解析分数范围（注意：ZREVRANGEBYSCORE 中 max 在前，min 在后）
	maxScore, maxInclusive := parseScore(max)
	minScore, minInclusive := parseScore(min)

	// 获取所有元素（反向）
	entries, _ := zset.Range(0, -1, true)
	results := make([]*protocol.RESPValue, 0)

	for _, entry := range entries {
		score := entry.Score()
		if (minInclusive && score >= minScore || !minInclusive && score > minScore) &&
			(maxInclusive && score <= maxScore || !maxInclusive && score < maxScore) {
			if offset > 0 {
				offset--
				continue
			}
			if count == 0 {
				break
			}
			results = append(results, protocol.NewBulkString(string(entry.Member())))
			if withScores {
				results = append(results, protocol.NewBulkString(strconv.FormatFloat(score, 'f', -1, 64)))
			}
			if count > 0 {
				count--
			}
		}
	}

	return protocol.NewArray(results)
}

func cmdZRemRangeByRank(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	start, err := strconv.Atoi(args[1].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2].ToString())
	if err != nil {
		return protocol.NewError("ERR value is not an integer or out of range")
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 获取所有元素
	entries, _ := zset.Range(0, -1, false)
	length := len(entries)

	// 处理负数索引
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// 边界检查
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return protocol.NewInteger(0)
	}

	// 删除范围内的元素
	removed := 0
	for i := start; i <= end; i++ {
		if i < len(entries) {
			zset.Remove(entries[i].Member())
			removed++
		}
	}

	return protocol.NewInteger(int64(removed))
}

func cmdZRemRangeByScore(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	min := args[1].ToString()
	max := args[2].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewInteger(0)
	}

	zset, err := obj.GetZSet()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 解析分数范围
	minScore, minInclusive := parseScore(min)
	maxScore, maxInclusive := parseScore(max)

	// 获取所有元素
	entries, _ := zset.Range(0, -1, false)
	removed := 0

	for _, entry := range entries {
		score := entry.Score()
		if (minInclusive && score >= minScore || !minInclusive && score > minScore) &&
			(maxInclusive && score <= maxScore || !maxInclusive && score < maxScore) {
			zset.Remove(entry.Member())
			removed++
		}
	}

	return protocol.NewInteger(int64(removed))
}

func cmdSort(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	if len(args) < 1 {
		return protocol.NewError("ERR wrong number of arguments for 'sort' command")
	}

	key := args[0].ToString()

	// 解析可选参数（简化实现）
	limitOffset := -1
	limitCount := -1
	getPatterns := make([]string, 0)
	order := "ASC" // ASC or DESC
	alpha := false
	store := ""

	// 解析参数
	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(args[i].ToString())
		switch arg {
		case "BY":
			if i+1 < len(args) {
				_ = args[i+1].ToString() // byPattern (未实现)
				i++
			}
		case "LIMIT":
			if i+2 < len(args) {
				limitOffset, _ = strconv.Atoi(args[i+1].ToString())
				limitCount, _ = strconv.Atoi(args[i+2].ToString())
				i += 2
			}
		case "GET":
			if i+1 < len(args) {
				getPatterns = append(getPatterns, args[i+1].ToString())
				i++
			}
		case "ASC":
			order = "ASC"
		case "DESC":
			order = "DESC"
		case "ALPHA":
			alpha = true
		case "STORE":
			if i+1 < len(args) {
				store = args[i+1].ToString()
				i++
			}
		}
	}

	// 获取源数据
	obj, err := ctx.Db.Get(key)
	if err != nil {
		if store != "" {
			ctx.Db.Del(store)
			return protocol.NewInteger(0)
		}
		return protocol.NewArray([]*protocol.RESPValue{})
	}

	// 根据类型排序
	var sortedValues [][]byte

	switch obj.Type {
	case storage.OBJ_LIST:
		list, _ := obj.GetList()
		values, _ := list.Range(0, -1)
		sortedValues = values
		// 排序
		if alpha {
			// 字母排序
			sort.Slice(sortedValues, func(i, j int) bool {
				if order == "DESC" {
					return string(sortedValues[i]) > string(sortedValues[j])
				}
				return string(sortedValues[i]) < string(sortedValues[j])
			})
		} else {
			// 数字排序
			sort.Slice(sortedValues, func(i, j int) bool {
				valI, _ := strconv.ParseFloat(string(sortedValues[i]), 64)
				valJ, _ := strconv.ParseFloat(string(sortedValues[j]), 64)
				if order == "DESC" {
					return valI > valJ
				}
				return valI < valJ
			})
		}

	case storage.OBJ_SET:
		set, _ := obj.GetSet()
		members := set.Members()
		sortedValues = members
		// 排序
		if alpha {
			sort.Slice(sortedValues, func(i, j int) bool {
				if order == "DESC" {
					return string(sortedValues[i]) > string(sortedValues[j])
				}
				return string(sortedValues[i]) < string(sortedValues[j])
			})
		} else {
			sort.Slice(sortedValues, func(i, j int) bool {
				valI, _ := strconv.ParseFloat(string(sortedValues[i]), 64)
				valJ, _ := strconv.ParseFloat(string(sortedValues[j]), 64)
				if order == "DESC" {
					return valI > valJ
				}
				return valI < valJ
			})
		}

	case storage.OBJ_ZSET:
		zset, _ := obj.GetZSet()
		entries, _ := zset.Range(0, -1, order == "DESC")
		sortedValues = make([][]byte, len(entries))
		for i, entry := range entries {
			sortedValues[i] = entry.Member()
		}

	default:
		return protocol.NewError("ERR One of the keys didn't contain a list, set or sorted set")
	}

	// 应用 LIMIT
	if limitOffset >= 0 && limitCount > 0 {
		if limitOffset < len(sortedValues) {
			end := limitOffset + limitCount
			if end > len(sortedValues) {
				end = len(sortedValues)
			}
			sortedValues = sortedValues[limitOffset:end]
		} else {
			sortedValues = [][]byte{}
		}
	}

	// 处理 GET 模式
	if len(getPatterns) > 0 {
		results := make([]*protocol.RESPValue, 0)
		for _, val := range sortedValues {
			for _, pattern := range getPatterns {
				// 替换 * 为实际值
				getKey := strings.Replace(pattern, "*", string(val), -1)
				getObj, err := ctx.Db.Get(getKey)
				if err != nil {
					results = append(results, protocol.NewNullBulkString())
				} else {
					getVal, _ := getObj.GetStringValue()
					results = append(results, protocol.NewBulkString(string(getVal)))
				}
			}
		}
		if store != "" {
			// STORE 模式：保存到列表
			storeListObj := storage.NewListObject()
			storeList, _ := storeListObj.GetList()
			for _, result := range results {
				if result.Type == protocol.RESP_BULK_STRING {
					storeList.Push([]byte(result.Str), 1)
				}
			}
			ctx.Db.Set(store, storeListObj)
			return protocol.NewInteger(int64(len(results)))
		}
		return protocol.NewArray(results)
	}

	// 直接返回排序后的值
	if store != "" {
		// STORE 模式：保存到列表
		storeListObj := storage.NewListObject()
		storeList, _ := storeListObj.GetList()
		for _, val := range sortedValues {
			storeList.Push(val, 1)
		}
		ctx.Db.Set(store, storeListObj)
		return protocol.NewInteger(int64(len(sortedValues)))
	}

	results := make([]*protocol.RESPValue, len(sortedValues))
	for i, val := range sortedValues {
		results[i] = protocol.NewBulkString(string(val))
	}
	return protocol.NewArray(results)
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

func cmdHMSet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	if (len(args)-1)%2 != 0 {
		return protocol.NewError("ERR wrong number of arguments for HMSET")
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

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		field := args[i].ToString()
		value := args[i+1].ToString()
		hash.Set([]byte(field), []byte(value))
	}

	return protocol.NewSimpleString("OK")
}

func cmdHMGet(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()

	obj, err := ctx.Db.Get(key)
	if err != nil {
		// 返回所有字段的 nil
		results := make([]*protocol.RESPValue, len(args)-1)
		for i := range results {
			results[i] = protocol.NewNullBulkString()
		}
		return protocol.NewArray(results)
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	results := make([]*protocol.RESPValue, len(args)-1)
	for i := 1; i < len(args); i++ {
		field := args[i].ToString()
		value, exists := hash.Get([]byte(field))
		if !exists {
			results[i-1] = protocol.NewNullBulkString()
		} else {
			results[i-1] = protocol.NewBulkString(string(value))
		}
	}

	return protocol.NewArray(results)
}

func cmdHSetNx(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	field := args[1].ToString()
	value := args[2].ToString()

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

	// 检查字段是否存在
	_, exists := hash.Get([]byte(field))
	if exists {
		return protocol.NewInteger(0)
	}

	// 设置字段
	hash.Set([]byte(field), []byte(value))
	return protocol.NewInteger(1)
}

func cmdHStrLen(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
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

	value, exists := hash.Get([]byte(field))
	if !exists {
		return protocol.NewInteger(0)
	}

	return protocol.NewInteger(int64(len(value)))
}

func cmdHIncrByFloat(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	field := args[1].ToString()
	increment, err := strconv.ParseFloat(args[2].ToString(), 64)
	if err != nil {
		return protocol.NewError("ERR value is not a valid float")
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

	// 获取当前值
	value, exists := hash.Get([]byte(field))
	var currentValue float64
	if !exists {
		currentValue = 0
	} else {
		parsed, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			return protocol.NewError("ERR hash value is not a valid float")
		}
		currentValue = parsed
	}

	// 计算新值
	newValue := currentValue + increment
	newValueStr := strconv.FormatFloat(newValue, 'f', -1, 64)
	hash.Set([]byte(field), []byte(newValueStr))

	return protocol.NewBulkString(newValueStr)
}

func cmdHScan(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	key := args[0].ToString()
	cursor, err := strconv.ParseInt(args[1].ToString(), 10, 64)
	if err != nil {
		return protocol.NewError("ERR invalid cursor")
	}

	obj, err := ctx.Db.Get(key)
	if err != nil {
		return protocol.NewArray([]*protocol.RESPValue{
			protocol.NewBulkString("0"),
			protocol.NewArray([]*protocol.RESPValue{}),
		})
	}

	hash, err := obj.GetHash()
	if err != nil {
		return protocol.NewError("ERR wrong type")
	}

	// 获取所有字段和值
	fields := hash.Keys()
	values := hash.Values()

	// 简化实现：返回所有字段值对
	if cursor >= int64(len(fields)) {
		return protocol.NewArray([]*protocol.RESPValue{
			protocol.NewBulkString("0"),
			protocol.NewArray([]*protocol.RESPValue{}),
		})
	}

	// 返回字段值对
	results := make([]*protocol.RESPValue, 0)
	for i := int(cursor); i < len(fields); i++ {
		results = append(results, protocol.NewBulkString(string(fields[i])))
		if i < len(values) {
			results = append(results, protocol.NewBulkString(string(values[i])))
		}
	}

	return protocol.NewArray([]*protocol.RESPValue{
		protocol.NewBulkString("0"), // 扫描完成
		protocol.NewArray(results),
	})
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
	if ctx.Server.cluster == nil {
		return protocol.NewError("ERR This instance has cluster support disabled")
	}

	if len(args) == 0 {
		return protocol.NewError("ERR wrong number of arguments for 'cluster' command")
	}

	subcommand := strings.ToUpper(args[0].ToString())

	switch subcommand {
	case "SLOTS":
		// 返回槽分配信息
		cluster := ctx.Server.cluster
		slots := make([]*protocol.RESPValue, 0)

		// 获取所有槽的分配信息（简化实现：只返回当前节点的槽）
		myself := cluster.GetMyself()
		if myself == nil {
			return protocol.NewArray([]*protocol.RESPValue{})
		}

		// 获取当前节点负责的槽
		mySlots := make([]int, 0)
		for slot := 0; slot < 16384; slot++ {
			node := cluster.GetSlotNode(slot)
			if node != nil {
				// 通过比较地址判断是否是当前节点
				if node.Addr == myself.Addr {
					mySlots = append(mySlots, slot)
				}
			}
		}

		if len(mySlots) == 0 {
			return protocol.NewArray([]*protocol.RESPValue{})
		}

		// 找到最小和最大槽
		minSlot := mySlots[0]
		maxSlot := mySlots[0]
		for _, s := range mySlots {
			if s < minSlot {
				minSlot = s
			}
			if s > maxSlot {
				maxSlot = s
			}
		}

		// 解析地址
		host := "127.0.0.1"
		port := "6379"
		if strings.Contains(myself.Addr, ":") {
			parts := strings.Split(myself.Addr, ":")
			if len(parts) == 2 {
				host = parts[0]
				port = parts[1]
			}
		}

		// 格式: [minSlot, maxSlot, [host, port, nodeID]]
		slotInfo := protocol.NewArray([]*protocol.RESPValue{
			protocol.NewInteger(int64(minSlot)),
			protocol.NewInteger(int64(maxSlot)),
			protocol.NewArray([]*protocol.RESPValue{
				protocol.NewBulkString(host),
				protocol.NewInteger(parsePort(port)),
				protocol.NewBulkString(myself.NodeID),
			}),
		})
		slots = append(slots, slotInfo)

		return protocol.NewArray(slots)

	case "NODES":
		// 返回节点信息（Redis 格式）
		cluster := ctx.Server.cluster
		nodes := cluster.GetNodes()

		var result strings.Builder
		for _, node := range nodes {
			// 格式: <node-id> <ip>:<port>@<cport> <flags> <master-id> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots>
			flags := "master"
			if node.Master != nil {
				flags = "slave"
			}

			masterID := "-"
			if node.Master != nil {
				masterID = node.Master.NodeID
			}

			slots := ""
			if len(node.Slots) > 0 {
				slots = fmt.Sprintf(" %d-%d", node.Slots[0], node.Slots[len(node.Slots)-1])
			}

			result.WriteString(fmt.Sprintf("%s %s@%s %s %s 0 0 0 connected%s\n",
				node.NodeID, node.Addr, "0", flags, masterID, slots))
		}

		return protocol.NewBulkString(result.String())

	case "MEET":
		// 节点握手
		if len(args) < 3 {
			return protocol.NewError("ERR wrong number of arguments for 'cluster|meet' command")
		}
		ip := args[1].ToString()
		port := args[2].ToString()
		addr := fmt.Sprintf("%s:%s", ip, port)

		// 添加节点
		nodeID := fmt.Sprintf("node-%s-%s", ip, port)
		ctx.Server.cluster.AddNode(nodeID, addr)

		// 发送 MEET 消息（简化实现）
		return protocol.NewSimpleString("OK")

	case "INFO":
		// 返回集群信息
		cluster := ctx.Server.cluster
		nodes := cluster.GetNodes()

		assignedSlots := 0
		for slot := 0; slot < 16384; slot++ {
			if cluster.GetSlotNode(slot) != nil {
				assignedSlots++
			}
		}

		info := fmt.Sprintf("cluster_state:ok\ncluster_slots_assigned:%d\ncluster_slots_ok:%d\ncluster_known_nodes:%d\ncluster_size:%d\n",
			assignedSlots, assignedSlots, len(nodes), len(nodes))
		return protocol.NewBulkString(info)

	case "ADDSLOTS":
		// 分配槽给当前节点
		if len(args) < 2 {
			return protocol.NewError("ERR wrong number of arguments for 'cluster|addslots' command")
		}

		slots := make([]int, 0)
		for i := 1; i < len(args); i++ {
			slot, err := strconv.Atoi(args[i].ToString())
			if err != nil || slot < 0 || slot >= 16384 {
				return protocol.NewError(fmt.Sprintf("ERR Invalid slot number: %s", args[i].ToString()))
			}
			slots = append(slots, slot)
		}

		myself := ctx.Server.cluster.GetMyself()
		ctx.Server.cluster.AssignSlots(myself.NodeID, slots)
		return protocol.NewSimpleString("OK")

	default:
		return protocol.NewError("ERR unknown subcommand or wrong number of arguments for 'cluster'")
	}
}

// parsePort 解析端口字符串
func parsePort(portStr string) int64 {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 6379
	}
	return int64(port)
}

// ========== 复制命令实现 ==========

func cmdReplConf(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	// REPLCONF 命令用于配置复制
	// 格式: REPLCONF <option> <value>
	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments for 'replconf' command")
	}

	option := args[0].ToString()
	option = strings.ToUpper(option)

	switch option {
	case "LISTENING-PORT":
		// 从节点报告监听端口
		// 简化实现：直接返回 OK
		return protocol.NewSimpleString("OK")
	case "CAPA":
		// 能力协商
		return protocol.NewSimpleString("OK")
	case "ACK":
		// 从节点确认接收到的数据量
		// 简化实现：直接返回 OK
		return protocol.NewSimpleString("OK")
	default:
		return protocol.NewSimpleString("OK")
	}
}

func cmdPSync(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	// PSYNC 命令用于部分同步或全量同步
	// 格式: PSYNC <replication-id> <offset>
	if ctx.Server.master == nil {
		return protocol.NewError("ERR server is not a master")
	}

	// 获取客户端连接
	if ctx.Client == nil {
		return protocol.NewError("ERR no client connection")
	}

	// 添加从节点（fullResync 会在 goroutine 中执行）
	_ = ctx.Server.master.AddReplica(ctx.Client.conn)

	// fullResync 会在 goroutine 中执行，这里不需要等待
	// 返回会被 fullResync 中的响应覆盖，但为了兼容性先返回 OK
	return protocol.NewSimpleString("OK")
}

func cmdSlaveOf(ctx *CommandContext, args []*protocol.RESPValue) *protocol.RESPValue {
	// SLAVEOF 命令用于将当前服务器设置为从节点
	// 格式: SLAVEOF <host> <port> 或 SLAVEOF NO ONE
	if len(args) < 2 {
		return protocol.NewError("ERR wrong number of arguments for 'slaveof' command")
	}

	host := args[0].ToString()
	port := args[1].ToString()

	if strings.ToUpper(host) == "NO" && strings.ToUpper(port) == "ONE" {
		// SLAVEOF NO ONE - 停止复制，成为主节点
		// 简化实现：直接返回 OK
		return protocol.NewSimpleString("OK")
	}

	// 连接到主节点
	masterAddr := fmt.Sprintf("%s:%s", host, port)
	slave := replication.NewSlave(masterAddr)
	if err := slave.Connect(); err != nil {
		return protocol.NewError(fmt.Sprintf("ERR failed to connect to master: %v", err))
	}

	// 简化实现：不保存 slave 引用，只返回 OK
	return protocol.NewSimpleString("OK")
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
