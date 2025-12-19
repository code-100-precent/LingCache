package structure

import (
	"encoding/binary"
	"errors"
)

/*
 * ============================================================================
 * Redis Set 数据结构 - Intset + Hashtable
 * ============================================================================
 *
 * 【核心原理】
 * Redis Set 使用两种编码方式：
 * 1. OBJ_ENCODING_INTSET: 整数集合（所有元素都是整数时使用）
 * 2. OBJ_ENCODING_HT: 哈希表（包含非整数元素或元素过多时使用）
 *
 * 【Intset 原理】
 * Intset 是一个有序的整数数组，使用变长编码：
 * 1. encoding 决定每个整数占用的字节数（int16/int32/int64）
 * 2. length 记录元素个数
 * 3. contents 是实际的整数数组（变长编码）
 *
 * 优势：
 * - 内存紧凑：没有指针开销，连续内存
 * - 有序：支持二分查找，O(log n) 查找
 * - 变长编码：根据最大值自动选择编码类型
 *
 * 【Hashtable 原理】
 * 当 Set 包含非整数或元素过多时，使用哈希表（dict）：
 * - key 是元素值，value 为 NULL（Set 只需要 key）
 * - O(1) 平均时间复杂度查找、插入、删除
 *
 * 【编码转换策略】
 * - 初始：如果所有元素都是整数，使用 intset
 * - 转换条件：
 *   a) 添加非整数元素 → 转换为 hashtable
 *   b) 元素数量超过 set_max_intset_entries（默认512）→ 转换为 hashtable
 * - 不会从 hashtable 转换回 intset（因为可能包含非整数）
 *
 * 【面试题】
 * Q1: 为什么 Set 要使用两种编码方式？
 * A1: 为了在不同场景下优化性能：
 *     - 小整数集合：intset 更节省内存，查找也很快（O(log n)）
 *     - 大集合或包含字符串：hashtable O(1) 查找，更高效
 *     根据数据特征动态选择最优编码
 *
 * Q2: Intset 的变长编码是什么？
 * A2: Intset 根据集合中最大整数选择编码：
 *     - 如果所有整数 < 2^15，使用 int16（每个元素2字节）
 *     - 如果所有整数 < 2^31，使用 int32（每个元素4字节）
 *     - 否则使用 int64（每个元素8字节）
 *     这样可以节省内存，例如只有小整数时不需要8字节
 *
 * Q3: Intset 为什么是有序的？
 * A3: 有序的好处：
 *     - 支持二分查找，O(log n) 查找
 *     - 去重简单（插入时检查相邻元素）
 *     - 支持范围操作（如 SINTER）
 *     插入时需要维护有序性，但查找很快
 *
 * Q4: Set 的编码转换是单向的吗？
 * A4: 是的，只能从 intset → hashtable，不能反向：
 *     - intset 只能存储整数
 *     - 一旦转换为 hashtable，可能包含非整数元素
 *     - 即使后来删除了所有非整数，也不会转换回去（避免频繁转换）
 *
 * Q5: Intset 升级（upgrade）是什么？
 * A5: 当添加一个超出当前编码范围的整数时，需要升级：
 *     - 例如：当前是 int16，添加一个 > 32767 的整数
 *     - 需要将所有元素从 int16 升级到 int32
 *     - 这是一个 O(n) 操作，但很少发生
 */

// SetEncoding 编码类型（使用 Encoding 别名）
// 常量定义在 encoding.go 中

const (
	INTSET_ENC_INT16 = 2 // int16 编码
	INTSET_ENC_INT32 = 4 // int32 编码
	INTSET_ENC_INT64 = 8 // int64 编码
)

const (
	SET_MAX_INTSET_ENTRIES = 512 // 超过此数量转换为 hashtable
)

// IntsetEncoding 整数集合编码类型
type IntsetEncoding byte

// Intset 整数集合
type Intset struct {
	encoding IntsetEncoding // 编码类型（2/4/8字节）
	length   uint32         // 元素个数
	contents []int64        // 整数数组（简化实现，实际是变长编码）
}

// RedisSet Redis Set 对象
type RedisSet struct {
	encoding  SetEncoding
	intset    *Intset
	hashtable map[string]bool // 简化实现，实际使用 dict
}

// NewSet 创建新的 Redis Set
func NewSet() *RedisSet {
	return &RedisSet{
		encoding:  OBJ_ENCODING_INTSET,
		intset:    &Intset{encoding: INTSET_ENC_INT16, length: 0, contents: make([]int64, 0)},
		hashtable: nil,
	}
}

// Add 添加元素到 Set
func (rs *RedisSet) Add(member []byte) error {
	if rs.encoding == OBJ_ENCODING_INTSET {
		return rs.addIntset(member)
	} else {
		return rs.addHashtable(member)
	}
}

// addIntset 向 intset 添加元素
func (rs *RedisSet) addIntset(member []byte) error {
	// 尝试解析为整数
	intVal, isInt := rs.parseInt(member)

	if !isInt {
		// 非整数，转换为 hashtable
		rs.convertToHashtable()
		return rs.addHashtable(member)
	}

	// 检查是否需要升级编码
	rs.intsetUpgrade(intVal)

	// 检查是否已存在（二分查找）
	idx, exists := rs.intsetSearch(intVal)
	if exists {
		return nil // 已存在，不重复添加
	}

	// 插入到有序位置
	rs.intset.contents = append(rs.intset.contents, 0)
	copy(rs.intset.contents[idx+1:], rs.intset.contents[idx:])
	rs.intset.contents[idx] = intVal
	rs.intset.length++

	// 检查是否需要转换为 hashtable
	if rs.intset.length > SET_MAX_INTSET_ENTRIES {
		rs.convertToHashtable()
	}

	return nil
}

// addHashtable 向 hashtable 添加元素
func (rs *RedisSet) addHashtable(member []byte) error {
	if rs.hashtable == nil {
		rs.hashtable = make(map[string]bool)
	}
	rs.hashtable[string(member)] = true
	return nil
}

// Remove 从 Set 删除元素
func (rs *RedisSet) Remove(member []byte) error {
	if rs.encoding == OBJ_ENCODING_INTSET {
		return rs.removeIntset(member)
	} else {
		return rs.removeHashtable(member)
	}
}

// removeIntset 从 intset 删除元素
func (rs *RedisSet) removeIntset(member []byte) error {
	intVal, isInt := rs.parseInt(member)
	if !isInt {
		return errors.New("member not found")
	}

	idx, exists := rs.intsetSearch(intVal)
	if !exists {
		return errors.New("member not found")
	}

	// 删除元素
	copy(rs.intset.contents[idx:], rs.intset.contents[idx+1:])
	rs.intset.contents = rs.intset.contents[:len(rs.intset.contents)-1]
	rs.intset.length--

	return nil
}

// removeHashtable 从 hashtable 删除元素
func (rs *RedisSet) removeHashtable(member []byte) error {
	if rs.hashtable == nil {
		return errors.New("member not found")
	}

	if _, exists := rs.hashtable[string(member)]; !exists {
		return errors.New("member not found")
	}

	delete(rs.hashtable, string(member))
	return nil
}

// IsMember 检查元素是否存在
func (rs *RedisSet) IsMember(member []byte) bool {
	if rs.encoding == OBJ_ENCODING_INTSET {
		return rs.isMemberIntset(member)
	} else {
		return rs.isMemberHashtable(member)
	}
}

// isMemberIntset 在 intset 中查找
func (rs *RedisSet) isMemberIntset(member []byte) bool {
	intVal, isInt := rs.parseInt(member)
	if !isInt {
		return false
	}

	_, exists := rs.intsetSearch(intVal)
	return exists
}

// isMemberHashtable 在 hashtable 中查找
func (rs *RedisSet) isMemberHashtable(member []byte) bool {
	if rs.hashtable == nil {
		return false
	}
	_, exists := rs.hashtable[string(member)]
	return exists
}

// Card 获取 Set 的元素数量
func (rs *RedisSet) Card() int {
	if rs.encoding == OBJ_ENCODING_INTSET {
		return int(rs.intset.length)
	} else {
		return len(rs.hashtable)
	}
}

// Members 获取所有成员
func (rs *RedisSet) Members() [][]byte {
	if rs.encoding == OBJ_ENCODING_INTSET {
		return rs.membersIntset()
	} else {
		return rs.membersHashtable()
	}
}

// membersIntset 获取 intset 的所有成员
func (rs *RedisSet) membersIntset() [][]byte {
	result := make([][]byte, 0, rs.intset.length)
	for _, val := range rs.intset.contents {
		result = append(result, rs.intToBytes(val))
	}
	return result
}

// membersHashtable 获取 hashtable 的所有成员
func (rs *RedisSet) membersHashtable() [][]byte {
	result := make([][]byte, 0, len(rs.hashtable))
	for member := range rs.hashtable {
		result = append(result, []byte(member))
	}
	return result
}

// parseInt 尝试将字节数组解析为整数
func (rs *RedisSet) parseInt(member []byte) (int64, bool) {
	// 简化实现：检查是否全是数字
	if len(member) == 0 {
		return 0, false
	}

	// 检查符号
	start := 0
	negative := false
	if member[0] == '-' {
		negative = true
		start = 1
	} else if member[0] == '+' {
		start = 1
	}

	if start >= len(member) {
		return 0, false
	}

	// 检查是否全是数字
	for i := start; i < len(member); i++ {
		if member[i] < '0' || member[i] > '9' {
			return 0, false
		}
	}

	// 解析整数（简化实现）
	var val int64
	for i := start; i < len(member); i++ {
		val = val*10 + int64(member[i]-'0')
	}

	if negative {
		val = -val
	}

	return val, true
}

// intToBytes 将整数转换为字节数组
func (rs *RedisSet) intToBytes(val int64) []byte {
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

// intsetSearch 在 intset 中二分查找
func (rs *RedisSet) intsetSearch(value int64) (int, bool) {
	contents := rs.intset.contents
	left, right := 0, len(contents)-1

	for left <= right {
		mid := (left + right) / 2
		if contents[mid] == value {
			return mid, true
		} else if contents[mid] < value {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return left, false
}

// intsetUpgrade 升级 intset 编码
func (rs *RedisSet) intsetUpgrade(value int64) {
	currentEnc := rs.intset.encoding
	var newEnc IntsetEncoding

	// 确定需要的编码
	if value >= -32768 && value <= 32767 {
		newEnc = INTSET_ENC_INT16
	} else if value >= -2147483648 && value <= 2147483647 {
		newEnc = INTSET_ENC_INT32
	} else {
		newEnc = INTSET_ENC_INT64
	}

	// 如果不需要升级，直接返回
	if newEnc <= currentEnc {
		return
	}

	// 升级编码（简化实现：实际需要重新编码所有元素）
	rs.intset.encoding = newEnc
}

// convertToHashtable 转换为 hashtable
func (rs *RedisSet) convertToHashtable() {
	if rs.encoding == OBJ_ENCODING_HT {
		return
	}

	rs.hashtable = make(map[string]bool)

	// 将 intset 中的所有元素添加到 hashtable
	for _, val := range rs.intset.contents {
		member := rs.intToBytes(val)
		rs.hashtable[string(member)] = true
	}

	rs.encoding = OBJ_ENCODING_HT
	rs.intset = nil
}

// RandomMember 随机获取一个成员
func (rs *RedisSet) RandomMember() []byte {
	if rs.encoding == OBJ_ENCODING_INTSET {
		if rs.intset.length == 0 {
			return nil
		}
		// 简化实现：返回第一个
		return rs.intToBytes(rs.intset.contents[0])
	} else {
		if len(rs.hashtable) == 0 {
			return nil
		}
		// 简化实现：返回第一个
		for member := range rs.hashtable {
			return []byte(member)
		}
	}
	return nil
}

// Inter 求交集
func (rs *RedisSet) Inter(others ...*RedisSet) *RedisSet {
	result := NewSet()

	// 获取第一个集合的所有成员
	members := rs.Members()

	// 检查每个成员是否在所有其他集合中
	for _, member := range members {
		allContain := true
		for _, other := range others {
			if !other.IsMember(member) {
				allContain = false
				break
			}
		}
		if allContain {
			result.Add(member)
		}
	}

	return result
}

// Union 求并集
func (rs *RedisSet) Union(others ...*RedisSet) *RedisSet {
	result := NewSet()

	// 添加当前集合的所有成员
	members := rs.Members()
	for _, member := range members {
		result.Add(member)
	}

	// 添加其他集合的成员
	for _, other := range others {
		otherMembers := other.Members()
		for _, member := range otherMembers {
			result.Add(member)
		}
	}

	return result
}

// Diff 求差集
func (rs *RedisSet) Diff(others ...*RedisSet) *RedisSet {
	result := NewSet()

	// 获取当前集合的所有成员
	members := rs.Members()

	// 检查每个成员是否不在任何其他集合中
	for _, member := range members {
		notInAny := true
		for _, other := range others {
			if other.IsMember(member) {
				notInAny = false
				break
			}
		}
		if notInAny {
			result.Add(member)
		}
	}

	return result
}

// 辅助函数：二进制编码/解码（用于实际 intset 实现）

// encodeIntsetValue 编码整数值（根据 encoding）
func encodeIntsetValue(value int64, encoding IntsetEncoding) []byte {
	buf := make([]byte, int(encoding))
	switch encoding {
	case INTSET_ENC_INT16:
		binary.BigEndian.PutUint16(buf, uint16(value))
	case INTSET_ENC_INT32:
		binary.BigEndian.PutUint32(buf, uint32(value))
	case INTSET_ENC_INT64:
		binary.BigEndian.PutUint64(buf, uint64(value))
	}
	return buf
}

// decodeIntsetValue 解码整数值
func decodeIntsetValue(data []byte, encoding IntsetEncoding) int64 {
	switch encoding {
	case INTSET_ENC_INT16:
		return int64(int16(binary.BigEndian.Uint16(data)))
	case INTSET_ENC_INT32:
		return int64(int32(binary.BigEndian.Uint32(data)))
	case INTSET_ENC_INT64:
		return int64(binary.BigEndian.Uint64(data))
	}
	return 0
}
