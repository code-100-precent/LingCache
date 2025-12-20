package structure

import (
	"bytes"
	"errors"
)

/*
 * ============================================================================
 * Redis Hash 数据结构 - Ziplist + Dict
 * ============================================================================
 *
 * 【核心原理】
 * Redis Hash 使用两种编码方式：
 * 1. OBJ_ENCODING_LISTPACK: 小哈希表使用 listpack（紧凑格式）
 * 2. OBJ_ENCODING_HT: 大哈希表使用 dict（哈希表）
 *
 * 【Listpack 存储格式】
 * 在 listpack 中，Hash 的 field-value 对按顺序存储：
 * [field1][value1][field2][value2]...
 *
 * 优势：
 * - 内存紧凑：没有指针开销
 * - 适合小数据：查找是 O(n)，但小数据时很快
 * - 缓存友好：连续内存，局部性好
 *
 * 【Dict 存储格式】
 * 当 Hash 变大时，使用 dict（哈希表）：
 * - key: field（字段名）
 * - value: value（字段值）
 * - O(1) 平均时间复杂度查找、插入、删除
 *
 * 【编码转换策略】
 * - 初始：小哈希表使用 listpack
 * - 转换条件：
 *   a) field 或 value 的大小超过 hash_max_listpack_value（默认64字节）
 *   b) 元素数量超过 hash_max_listpack_entries（默认512）
 * - 不会从 dict 转换回 listpack
 *
 * 【面试题】
 * Q1: 为什么 Hash 要使用两种编码方式？
 * A1: 为了在不同场景下优化性能：
 *     - 小哈希表：listpack 更节省内存，查找也很快（O(n) 但 n 很小）
 *     - 大哈希表：dict O(1) 查找，更高效
 *     根据数据规模动态选择最优编码
 *
 * Q2: Listpack 中 Hash 的 field-value 是如何存储的？
 * A2: 按顺序存储，field 和 value 交替出现：
 *     [field1][value1][field2][value2]...
 *     查找时需要遍历，但小数据时性能可以接受
 *
 * Q3: Hash 的编码转换是单向的吗？
 * A3: 是的，只能从 listpack → dict，不能反向：
 *     - 一旦转换为 dict，可能包含大字段或大量元素
 *     - 即使后来删除了大字段，也不会转换回去（避免频繁转换）
 *
 * Q4: Hash 和 String 有什么区别？
 * A4: Hash 适合存储对象（多个字段）：
 *     - Hash: HSET user:1 name "Alice" age "30"
 *     - String: SET user:1:name "Alice" SET user:1:age "30"
 *     Hash 的优势：
 *     - 原子操作：可以同时操作多个字段
 *     - 内存效率：小对象时更节省内存
 *     - 操作方便：HGETALL 一次获取所有字段
 *
 * Q5: Hash 的渐进式 rehash 是什么？
 * A5: 当 dict 需要扩容时，Redis 使用渐进式 rehash：
 *     - 不是一次性迁移所有数据，而是分多次
 *     - 每次操作时迁移一部分数据
 *     - 这样避免阻塞，保证响应时间
 *     这是 dict 的特性，Hash 作为使用者自动获得
 */

// HashEncoding 编码类型（使用 Encoding 别名）
// 常量定义在 encoding.go 中

const (
	HASH_MAX_LISTPACK_ENTRIES = 512 // 超过此数量转换为 dict
	HASH_MAX_LISTPACK_VALUE   = 64  // 超过此大小转换为 dict
)

// HashEntry 哈希表条目
type HashEntry struct {
	field []byte
	value []byte
}

// Field 获取 field
func (e *HashEntry) Field() []byte {
	return e.field
}

// Value 获取 value
func (e *HashEntry) Value() []byte {
	return e.value
}

// RedisHash Redis Hash 对象
type RedisHash struct {
	encoding  HashEncoding
	listpack  *ListpackFull     // 小哈希表使用 ListpackFull（存储 field-value 对）
	hashtable map[string][]byte // 大哈希表使用（简化实现，实际使用 dict）
}

// NewHash 创建新的 Redis Hash
func NewHash() *RedisHash {
	return &RedisHash{
		encoding:  OBJ_ENCODING_LISTPACK,
		listpack:  NewListpackFull(256),
		hashtable: nil,
	}
}

// Set 设置字段值
func (rh *RedisHash) Set(field, value []byte) error {
	if rh.encoding == OBJ_ENCODING_LISTPACK {
		return rh.setListpack(field, value)
	} else {
		return rh.setHashtable(field, value)
	}
}

// setListpack 在 listpack 中设置字段
func (rh *RedisHash) setListpack(field, value []byte) error {
	if rh.listpack == nil {
		rh.listpack = NewListpackFull(256)
	}

	// 检查是否需要转换（listpack 中 field-value 对算作 2 个元素）
	if rh.listpack.Length()/2 >= HASH_MAX_LISTPACK_ENTRIES ||
		len(field) > HASH_MAX_LISTPACK_VALUE ||
		len(value) > HASH_MAX_LISTPACK_VALUE {
		rh.convertToHashtable()
		return rh.setHashtable(field, value)
	}

	// 查找字段是否存在（field-value 成对存储）
	fieldIdx := rh.findFieldInListpack(field)

	if fieldIdx >= 0 {
		// 更新现有字段的值（fieldIdx 是 field 的位置，fieldIdx+1 是 value 的位置）
		// 需要重建 listpack 来更新值
		rh.updateFieldValue(fieldIdx, value)
	} else {
		// 添加新字段（field 和 value 都追加）
		rh.listpack.AppendString(field)
		rh.listpack.AppendString(value)
	}

	return nil
}

// findFieldInListpack 在 listpack 中查找字段（返回 field 的索引位置，-1 表示不存在）
func (rh *RedisHash) findFieldInListpack(field []byte) int {
	if rh.listpack == nil {
		return -1
	}

	idx := 0
	p := rh.listpack.First()
	for p != nil {
		sval, _, isInt, err := rh.listpack.GetValue(p)
		if err != nil {
			break
		}
		// 只检查 field（偶数索引位置）
		if idx%2 == 0 && !isInt && bytes.Equal(sval, field) {
			return idx
		}
		var nextErr error
		p, nextErr = rh.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
	}

	return -1
}

// updateFieldValue 更新字段的值（需要重建 listpack）
func (rh *RedisHash) updateFieldValue(fieldIdx int, newValue []byte) {
	// 收集所有 field-value 对
	entries := make([]HashEntry, 0, rh.listpack.Length()/2)

	p := rh.listpack.First()
	idx := 0
	var currentField []byte

	for p != nil {
		sval, _, _, err := rh.listpack.GetValue(p)
		if err != nil {
			break
		}

		if idx%2 == 0 {
			// field
			currentField = sval
		} else {
			// value
			if idx == fieldIdx+1 {
				// 这是要更新的值
				entries = append(entries, HashEntry{
					field: currentField,
					value: newValue,
				})
			} else {
				entries = append(entries, HashEntry{
					field: currentField,
					value: sval,
				})
			}
		}

		var nextErr error
		p, nextErr = rh.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
	}

	// 重建 listpack
	rh.listpack = NewListpackFull(256)
	for _, entry := range entries {
		rh.listpack.AppendString(entry.field)
		rh.listpack.AppendString(entry.value)
	}
}

// setHashtable 在 hashtable 中设置字段
func (rh *RedisHash) setHashtable(field, value []byte) error {
	if rh.hashtable == nil {
		rh.hashtable = make(map[string][]byte)
	}

	// 复制 value（避免外部修改）
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	rh.hashtable[string(field)] = valueCopy
	return nil
}

// Get 获取字段值
func (rh *RedisHash) Get(field []byte) ([]byte, bool) {
	if rh.encoding == OBJ_ENCODING_LISTPACK {
		return rh.getListpack(field)
	} else {
		return rh.getHashtable(field)
	}
}

// getListpack 从 listpack 获取字段
func (rh *RedisHash) getListpack(field []byte) ([]byte, bool) {
	if rh.listpack == nil {
		return nil, false
	}

	fieldIdx := rh.findFieldInListpack(field)
	if fieldIdx < 0 {
		return nil, false
	}

	// fieldIdx 是 field 的位置，fieldIdx+1 是 value 的位置
	// 需要找到 value
	p := rh.listpack.First()
	idx := 0
	for p != nil && idx <= fieldIdx+1 {
		if idx == fieldIdx+1 {
			// 这是 value
			sval, ival, isInt, err := rh.listpack.GetValue(p)
			if err != nil {
				return nil, false
			}
			if isInt {
				// 整数转换为字符串
				return rh.intToBytes(ival), true
			}
			return sval, true
		}
		var err error
		p, err = rh.listpack.Next(p)
		if err != nil || p == nil {
			break
		}
		idx++
	}

	return nil, false
}

// getHashtable 从 hashtable 获取字段
func (rh *RedisHash) getHashtable(field []byte) ([]byte, bool) {
	if rh.hashtable == nil {
		return nil, false
	}
	value, exists := rh.hashtable[string(field)]
	if !exists {
		return nil, false
	}
	// 返回副本（避免外部修改）
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy, true
}

// Del 删除字段
func (rh *RedisHash) Del(field []byte) error {
	if rh.encoding == OBJ_ENCODING_LISTPACK {
		return rh.delListpack(field)
	} else {
		return rh.delHashtable(field)
	}
}

// delListpack 从 listpack 删除字段
func (rh *RedisHash) delListpack(field []byte) error {
	if rh.listpack == nil {
		return errors.New("field not found")
	}

	fieldIdx := rh.findFieldInListpack(field)
	if fieldIdx < 0 {
		return errors.New("field not found")
	}

	// 收集所有 field-value 对，跳过要删除的
	entries := make([]HashEntry, 0, rh.listpack.Length()/2)

	p := rh.listpack.First()
	idx := 0
	var currentField []byte

	for p != nil {
		sval, _, _, err := rh.listpack.GetValue(p)
		if err != nil {
			break
		}

		if idx%2 == 0 {
			// field
			if idx == fieldIdx {
				// 跳过这个 field 和它的 value
				var nextErr error
				p, nextErr = rh.listpack.Next(p)
				if nextErr != nil || p == nil {
					break
				}
				idx++ // 跳过 value
				continue
			}
			currentField = sval
		} else {
			// value
			entries = append(entries, HashEntry{
				field: currentField,
				value: sval,
			})
		}

		var nextErr error
		p, nextErr = rh.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
	}

	// 重建 listpack
	rh.listpack = NewListpackFull(256)
	for _, entry := range entries {
		rh.listpack.AppendString(entry.field)
		rh.listpack.AppendString(entry.value)
	}

	return nil
}

// delHashtable 从 hashtable 删除字段
func (rh *RedisHash) delHashtable(field []byte) error {
	if rh.hashtable == nil {
		return errors.New("field not found")
	}

	if _, exists := rh.hashtable[string(field)]; !exists {
		return errors.New("field not found")
	}

	delete(rh.hashtable, string(field))
	return nil
}

// Exists 检查字段是否存在
func (rh *RedisHash) Exists(field []byte) bool {
	if rh.encoding == OBJ_ENCODING_LISTPACK {
		return rh.findFieldInListpack(field) >= 0
	} else {
		if rh.hashtable == nil {
			return false
		}
		_, exists := rh.hashtable[string(field)]
		return exists
	}
}

// convertToHashtable 转换为 hashtable
func (rh *RedisHash) convertToHashtable() {
	if rh.encoding == OBJ_ENCODING_HT {
		return
	}

	rh.hashtable = make(map[string][]byte)

	// 将 listpack 中的所有字段添加到 hashtable
	if rh.listpack != nil {
		p := rh.listpack.First()
		idx := 0
		var currentField []byte

		for p != nil {
			sval, _, _, err := rh.listpack.GetValue(p)
			if err != nil {
				break
			}

			if idx%2 == 0 {
				// field
				currentField = sval
			} else {
				// value
				valueCopy := make([]byte, len(sval))
				copy(valueCopy, sval)
				rh.hashtable[string(currentField)] = valueCopy
			}

			var nextErr error
			p, nextErr = rh.listpack.Next(p)
			if nextErr != nil || p == nil {
				break
			}
			idx++
		}
	}

	rh.encoding = OBJ_ENCODING_HT
	rh.listpack = nil
}

// Len 获取字段数量
func (rh *RedisHash) Len() int {
	if rh.encoding == OBJ_ENCODING_LISTPACK {
		if rh.listpack == nil {
			return 0
		}
		// listpack 中 field-value 成对存储，所以长度除以 2
		return int(rh.listpack.Length()) / 2
	} else {
		return len(rh.hashtable)
	}
}

// GetAll 获取所有字段值对
func (rh *RedisHash) GetAll() []HashEntry {
	if rh.encoding == OBJ_ENCODING_LISTPACK {
		return rh.getAllListpack()
	} else {
		return rh.getAllHashtable()
	}
}

// getAllListpack 从 listpack 获取所有字段
func (rh *RedisHash) getAllListpack() []HashEntry {
	if rh.listpack == nil {
		return []HashEntry{}
	}

	result := make([]HashEntry, 0, rh.listpack.Length()/2)
	p := rh.listpack.First()
	idx := 0
	var currentField []byte

	for p != nil {
		sval, _, _, err := rh.listpack.GetValue(p)
		if err != nil {
			break
		}

		if idx%2 == 0 {
			// field
			currentField = make([]byte, len(sval))
			copy(currentField, sval)
		} else {
			// value
			valueCopy := make([]byte, len(sval))
			copy(valueCopy, sval)
			result = append(result, HashEntry{
				field: currentField,
				value: valueCopy,
			})
		}

		var nextErr error
		p, nextErr = rh.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
	}

	return result
}

// getAllHashtable 从 hashtable 获取所有字段
func (rh *RedisHash) getAllHashtable() []HashEntry {
	if rh.hashtable == nil {
		return []HashEntry{}
	}

	result := make([]HashEntry, 0, len(rh.hashtable))
	for field, value := range rh.hashtable {
		fieldCopy := []byte(field)
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		result = append(result, HashEntry{
			field: fieldCopy,
			value: valueCopy,
		})
	}
	return result
}

// Keys 获取所有字段名
func (rh *RedisHash) Keys() [][]byte {
	if rh.encoding == OBJ_ENCODING_LISTPACK {
		return rh.keysListpack()
	} else {
		return rh.keysHashtable()
	}
}

// keysListpack 从 listpack 获取所有字段名
func (rh *RedisHash) keysListpack() [][]byte {
	if rh.listpack == nil {
		return [][]byte{}
	}

	result := make([][]byte, 0, rh.listpack.Length()/2)
	p := rh.listpack.First()
	idx := 0

	for p != nil {
		sval, _, isInt, err := rh.listpack.GetValue(p)
		if err != nil {
			break
		}

		// 只收集 field（偶数索引）
		if idx%2 == 0 && !isInt {
			fieldCopy := make([]byte, len(sval))
			copy(fieldCopy, sval)
			result = append(result, fieldCopy)
		}

		var nextErr error
		p, nextErr = rh.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
	}

	return result
}

// keysHashtable 从 hashtable 获取所有字段名
func (rh *RedisHash) keysHashtable() [][]byte {
	if rh.hashtable == nil {
		return [][]byte{}
	}

	result := make([][]byte, 0, len(rh.hashtable))
	for field := range rh.hashtable {
		result = append(result, []byte(field))
	}
	return result
}

// Values 获取所有字段值
func (rh *RedisHash) Values() [][]byte {
	if rh.encoding == OBJ_ENCODING_LISTPACK {
		return rh.valuesListpack()
	} else {
		return rh.valuesHashtable()
	}
}

// valuesListpack 从 listpack 获取所有字段值
func (rh *RedisHash) valuesListpack() [][]byte {
	if rh.listpack == nil {
		return [][]byte{}
	}

	result := make([][]byte, 0, rh.listpack.Length()/2)
	p := rh.listpack.First()
	idx := 0

	for p != nil {
		sval, _, _, err := rh.listpack.GetValue(p)
		if err != nil {
			break
		}

		// 只收集 value（奇数索引）
		if idx%2 == 1 {
			valueCopy := make([]byte, len(sval))
			copy(valueCopy, sval)
			result = append(result, valueCopy)
		}

		var nextErr error
		p, nextErr = rh.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
	}

	return result
}

// valuesHashtable 从 hashtable 获取所有字段值
func (rh *RedisHash) valuesHashtable() [][]byte {
	if rh.hashtable == nil {
		return [][]byte{}
	}

	result := make([][]byte, 0, len(rh.hashtable))
	for _, value := range rh.hashtable {
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		result = append(result, valueCopy)
	}
	return result
}

// IncrBy 将字段值增加指定数值
func (rh *RedisHash) IncrBy(field []byte, increment int64) (int64, error) {
	value, exists := rh.Get(field)

	var currentVal int64
	if exists {
		// 尝试解析为整数
		parsed, err := rh.parseInt(value)
		if err != nil {
			return 0, errors.New("value is not an integer")
		}
		currentVal = parsed
	}

	newVal := currentVal + increment
	newValBytes := rh.intToBytes(newVal)

	err := rh.Set(field, newValBytes)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

// IncrByFloat 将字段值增加指定浮点数
func (rh *RedisHash) IncrByFloat(field []byte, increment float64) (float64, error) {
	value, exists := rh.Get(field)

	var currentVal float64
	if exists {
		// 尝试解析为浮点数
		parsed, err := rh.parseFloat(value)
		if err != nil {
			return 0, errors.New("value is not a number")
		}
		currentVal = parsed
	}

	newVal := currentVal + increment
	newValBytes := rh.floatToBytes(newVal)

	err := rh.Set(field, newValBytes)
	if err != nil {
		return 0, err
	}

	return newVal, nil
}

// parseInt 解析整数（简化实现）
func (rh *RedisHash) parseInt(data []byte) (int64, error) {
	if len(data) == 0 {
		return 0, errors.New("empty string")
	}

	negative := false
	start := 0

	if data[0] == '-' {
		negative = true
		start = 1
	} else if data[0] == '+' {
		start = 1
	}

	if start >= len(data) {
		return 0, errors.New("invalid number")
	}

	var result int64
	for i := start; i < len(data); i++ {
		if data[i] < '0' || data[i] > '9' {
			return 0, errors.New("invalid number")
		}
		result = result*10 + int64(data[i]-'0')
	}

	if negative {
		result = -result
	}

	return result, nil
}

// parseFloat 解析浮点数（简化实现）
func (rh *RedisHash) parseFloat(data []byte) (float64, error) {
	// 简化实现：先尝试解析为整数
	val, err := rh.parseInt(data)
	if err == nil {
		return float64(val), nil
	}

	// 实际应该使用 strconv.ParseFloat
	// 这里简化处理
	return 0, errors.New("invalid float")
}

// intToBytes 整数转字节数组（辅助函数，用于从 listpack 获取整数时转换）
func (rh *RedisHash) intToBytes(val int64) []byte {
	if val == 0 {
		return []byte("0")
	}

	negative := val < 0
	if negative {
		val = -val
	}

	// 计算位数
	digits := 0
	temp := val
	for temp > 0 {
		digits++
		temp /= 10
	}

	result := make([]byte, digits)
	if negative {
		result = make([]byte, digits+1)
		result[0] = '-'
		digits++
	}

	idx := digits - 1
	for val > 0 {
		result[idx] = byte('0' + val%10)
		val /= 10
		idx--
	}

	return result
}

// floatToBytes 浮点数转字节数组（简化实现）
func (rh *RedisHash) floatToBytes(val float64) []byte {
	// 简化实现：转换为整数
	return rh.intToBytes(int64(val))
}

// MSet 批量设置字段
func (rh *RedisHash) MSet(fields, values [][]byte) error {
	if len(fields) != len(values) {
		return errors.New("fields and values length mismatch")
	}

	for i := 0; i < len(fields); i++ {
		if err := rh.Set(fields[i], values[i]); err != nil {
			return err
		}
	}

	return nil
}

// MGet 批量获取字段值
func (rh *RedisHash) MGet(fields [][]byte) [][]byte {
	result := make([][]byte, 0, len(fields))
	for _, field := range fields {
		value, exists := rh.Get(field)
		if exists {
			result = append(result, value)
		} else {
			result = append(result, nil)
		}
	}
	return result
}
