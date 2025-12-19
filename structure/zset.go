package structure

import (
	"bytes"
	"errors"
	"math/rand"
)

/*
 * ============================================================================
 * Redis Sorted Set (ZSet) 数据结构 - Skiplist + Dict
 * ============================================================================
 *
 * 【核心原理】
 * Redis ZSet 使用两种编码方式：
 * 1. OBJ_ENCODING_LISTPACK: 小有序集合使用 listpack（紧凑格式）
 * 2. OBJ_ENCODING_SKIPLIST: 大有序集合使用 skiplist + dict
 *
 * 【Skiplist 原理】
 * Skiplist（跳表）是一个多层的有序链表：
 * 1. 底层是完整的有序链表，包含所有元素
 * 2. 上层是"快速通道"，通过随机概率决定节点层数
 * 3. 查找时从顶层开始，逐层向下，类似二分查找
 *
 * 优势：
 * - 平均 O(log n) 查找、插入、删除
 * - 实现简单，比平衡树更容易
 * - 支持范围查询（ZRANGE）
 * - 内存开销：平均每个节点 1/(1-p) 个指针（p 是上层概率，通常 0.25）
 *
 * 【Dict 的作用】
 * ZSet 同时使用 dict（哈希表）存储 member -> score 的映射：
 * - 作用：O(1) 时间复杂度获取 member 的 score
 * - 如果没有 dict，需要 O(log n) 在 skiplist 中查找
 * - 空间换时间：多一份存储，但查询更快
 *
 * 【编码转换策略】
 * - 初始：小集合使用 listpack
 * - 转换条件：
 *   a) 元素数量超过 zset_max_listpack_entries（默认128）
 *   b) 元素大小超过 zset_max_listpack_value（默认64字节）
 * - 不会从 skiplist 转换回 listpack
 *
 * 【面试题】
 * Q1: 为什么 ZSet 要同时使用 Skiplist 和 Dict？
 * A1: 两种数据结构互补：
 *     - Skiplist: 支持范围查询（ZRANGE）、按 score 排序
 *     - Dict: 支持 O(1) 获取 member 的 score（ZSCORE）
 *     如果只用 Skiplist，ZSCORE 需要 O(log n) 查找
 *     如果只用 Dict，无法高效支持范围查询
 *
 * Q2: Skiplist 的层数是如何确定的？
 * A2: 使用随机算法：
 *     - 每个节点有 50% 概率进入上一层
 *     - 层数从 1 开始，每次有 50% 概率 +1
 *     - 最大层数通常限制为 32（Redis 默认）
 *     - 期望层数：E[L] = 1/(1-p) ≈ 2（p=0.5）
 *
 * Q3: Skiplist 和平衡树（如红黑树）有什么区别？
 * A3: Skiplist 的优势：
 *     - 实现简单，代码量少
 *     - 范围查询更高效（链表结构）
 *     - 并发友好（可以用无锁数据结构）
 *     平衡树的优势：
 *     - 最坏情况性能有保证（Skiplist 是概率性的）
 *     - 内存开销更小（不需要多个指针层）
 *
 * Q4: ZSet 的 score 可以重复吗？member 呢？
 * A4: - score 可以重复（多个 member 可以有相同 score）
 *     - member 不能重复（相同 member 会更新 score）
 *     当 score 相同时，按 member 的字典序排序
 *
 * Q5: ZSet 的范围查询是如何实现的？
 * A5: 使用 Skiplist 的链表特性：
 *     - ZRANGE: 从指定位置开始，沿着底层链表遍历
 *     - ZRANGEBYSCORE: 先定位到 score 范围的起点，然后遍历
 *     时间复杂度：O(log n + m)，n 是总数，m 是返回的元素数
 */

// ZSetEncoding 编码类型（使用 Encoding 别名）
// 常量定义在 encoding.go 中

const (
	ZSET_MAX_LISTPACK_ENTRIES = 128  // 超过此数量转换为 skiplist
	ZSET_MAX_LISTPACK_VALUE   = 64   // 超过此大小转换为 skiplist
	SKIPLIST_MAXLEVEL         = 32   // 最大层数
	SKIPLIST_P                = 0.25 // 上层概率
)

// SkipListNode 跳表节点
type SkipListNode struct {
	member   []byte
	score    float64
	backward *SkipListNode   // 后向指针（用于反向遍历）
	level    []SkipListLevel // 前向指针数组
}

// SkipListLevel 跳表层
type SkipListLevel struct {
	forward *SkipListNode // 前向指针
	span    uint32        // 跨度（用于排名）
}

// SkipList 跳表
type SkipList struct {
	header *SkipListNode // 头节点
	tail   *SkipListNode // 尾节点
	length uint32        // 节点数量
	level  int           // 当前最大层数
}

// RedisZSet Redis Sorted Set 对象
type RedisZSet struct {
	encoding ZSetEncoding
	listpack *ZSetListpack      // 小集合使用
	skiplist *SkipList          // 大集合使用
	dict     map[string]float64 // member -> score 映射（简化实现）
}

// ZSetListpack 有序集合的 listpack（简化实现）
type ZSetListpack struct {
	entries []ZSetEntry // (member, score) 对，按 score 排序
}

// ZSetEntry 有序集合条目
type ZSetEntry struct {
	member []byte
	score  float64
}

// Member 获取 member
func (e *ZSetEntry) Member() []byte {
	return e.member
}

// Score 获取 score
func (e *ZSetEntry) Score() float64 {
	return e.score
}

// NewZSet 创建新的 Redis ZSet
func NewZSet() *RedisZSet {
	return &RedisZSet{
		encoding: OBJ_ENCODING_LISTPACK,
		listpack: &ZSetListpack{
			entries: make([]ZSetEntry, 0),
		},
		skiplist: nil,
		dict:     nil,
	}
}

// Add 添加元素到 ZSet
func (rz *RedisZSet) Add(member []byte, score float64) error {
	if rz.encoding == OBJ_ENCODING_LISTPACK {
		return rz.addListpack(member, score)
	} else {
		return rz.addSkiplist(member, score)
	}
}

// addListpack 向 listpack 添加元素
func (rz *RedisZSet) addListpack(member []byte, score float64) error {
	// 检查是否需要转换
	if len(rz.listpack.entries) >= ZSET_MAX_LISTPACK_ENTRIES ||
		len(member) > ZSET_MAX_LISTPACK_VALUE {
		rz.convertToSkiplist()
		return rz.addSkiplist(member, score)
	}

	// 查找插入位置（保持有序）
	idx := rz.findInsertPosition(score, member)

	// 检查是否已存在
	if idx < len(rz.listpack.entries) &&
		rz.listpack.entries[idx].score == score &&
		bytes.Equal(rz.listpack.entries[idx].member, member) {
		// 更新 score
		rz.listpack.entries[idx].score = score
		return nil
	}

	// 插入新元素
	entry := ZSetEntry{member: member, score: score}
	rz.listpack.entries = append(rz.listpack.entries, ZSetEntry{})
	copy(rz.listpack.entries[idx+1:], rz.listpack.entries[idx:])
	rz.listpack.entries[idx] = entry

	return nil
}

// addSkiplist 向 skiplist 添加元素
func (rz *RedisZSet) addSkiplist(member []byte, score float64) error {
	if rz.skiplist == nil {
		rz.skiplist = newSkipList()
		rz.dict = make(map[string]float64)
	}

	// 检查是否已存在
	oldScore, exists := rz.dict[string(member)]
	if exists && oldScore == score {
		return nil // 已存在且 score 相同
	}

	// 如果存在，先删除
	if exists {
		rz.skiplist.Delete(member, oldScore)
	}

	// 插入到 skiplist
	rz.skiplist.Insert(member, score)

	// 更新 dict
	rz.dict[string(member)] = score

	return nil
}

// Remove 从 ZSet 删除元素
func (rz *RedisZSet) Remove(member []byte) error {
	if rz.encoding == OBJ_ENCODING_LISTPACK {
		return rz.removeListpack(member)
	} else {
		return rz.removeSkiplist(member)
	}
}

// removeListpack 从 listpack 删除元素
func (rz *RedisZSet) removeListpack(member []byte) error {
	for i, entry := range rz.listpack.entries {
		if bytes.Equal(entry.member, member) {
			// 删除元素
			copy(rz.listpack.entries[i:], rz.listpack.entries[i+1:])
			rz.listpack.entries = rz.listpack.entries[:len(rz.listpack.entries)-1]
			return nil
		}
	}
	return errors.New("member not found")
}

// removeSkiplist 从 skiplist 删除元素
func (rz *RedisZSet) removeSkiplist(member []byte) error {
	score, exists := rz.dict[string(member)]
	if !exists {
		return errors.New("member not found")
	}

	rz.skiplist.Delete(member, score)
	delete(rz.dict, string(member))

	return nil
}

// Score 获取 member 的 score
func (rz *RedisZSet) Score(member []byte) (float64, bool) {
	if rz.encoding == OBJ_ENCODING_LISTPACK {
		return rz.scoreListpack(member)
	} else {
		return rz.scoreSkiplist(member)
	}
}

// scoreListpack 从 listpack 获取 score
func (rz *RedisZSet) scoreListpack(member []byte) (float64, bool) {
	for _, entry := range rz.listpack.entries {
		if bytes.Equal(entry.member, member) {
			return entry.score, true
		}
	}
	return 0, false
}

// scoreSkiplist 从 skiplist 获取 score（使用 dict，O(1)）
func (rz *RedisZSet) scoreSkiplist(member []byte) (float64, bool) {
	score, exists := rz.dict[string(member)]
	return score, exists
}

// Rank 获取 member 的排名（从 0 开始）
func (rz *RedisZSet) Rank(member []byte, reverse bool) (int, bool) {
	if rz.encoding == OBJ_ENCODING_LISTPACK {
		return rz.rankListpack(member, reverse)
	} else {
		return rz.rankSkiplist(member, reverse)
	}
}

// rankListpack 从 listpack 获取排名
func (rz *RedisZSet) rankListpack(member []byte, reverse bool) (int, bool) {
	for i, entry := range rz.listpack.entries {
		if bytes.Equal(entry.member, member) {
			if reverse {
				return len(rz.listpack.entries) - 1 - i, true
			}
			return i, true
		}
	}
	return 0, false
}

// rankSkiplist 从 skiplist 获取排名
func (rz *RedisZSet) rankSkiplist(member []byte, reverse bool) (int, bool) {
	score, exists := rz.dict[string(member)]
	if !exists {
		return 0, false
	}

	// 简化实现：遍历计算排名
	rank := 0
	node := rz.skiplist.header.level[0].forward
	for node != nil {
		if bytes.Equal(node.member, member) {
			if reverse {
				return int(rz.skiplist.length) - 1 - rank, true
			}
			return rank, true
		}
		if node.score < score || (node.score == score && bytes.Compare(node.member, member) < 0) {
			rank++
		}
		node = node.level[0].forward
	}

	return 0, false
}

// Range 获取指定范围的元素
func (rz *RedisZSet) Range(start, end int, reverse bool) ([]ZSetEntry, error) {
	if rz.encoding == OBJ_ENCODING_LISTPACK {
		return rz.rangeListpack(start, end, reverse)
	} else {
		return rz.rangeSkiplist(start, end, reverse)
	}
}

// rangeListpack 从 listpack 获取范围
func (rz *RedisZSet) rangeListpack(start, end int, reverse bool) ([]ZSetEntry, error) {
	length := len(rz.listpack.entries)

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
		return []ZSetEntry{}, nil
	}

	result := make([]ZSetEntry, 0, end-start+1)
	if reverse {
		for i := end; i >= start; i-- {
			result = append(result, rz.listpack.entries[i])
		}
	} else {
		for i := start; i <= end; i++ {
			result = append(result, rz.listpack.entries[i])
		}
	}

	return result, nil
}

// rangeSkiplist 从 skiplist 获取范围
func (rz *RedisZSet) rangeSkiplist(start, end int, reverse bool) ([]ZSetEntry, error) {
	length := int(rz.skiplist.length)

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
		return []ZSetEntry{}, nil
	}

	result := make([]ZSetEntry, 0, end-start+1)

	if reverse {
		// 反向遍历
		node := rz.skiplist.tail
		idx := length - 1
		for node != nil && idx >= start {
			if idx <= end {
				result = append(result, ZSetEntry{
					member: node.member,
					score:  node.score,
				})
			}
			node = node.backward
			idx--
		}
	} else {
		// 正向遍历
		node := rz.skiplist.header.level[0].forward
		idx := 0
		for node != nil && idx <= end {
			if idx >= start {
				result = append(result, ZSetEntry{
					member: node.member,
					score:  node.score,
				})
			}
			node = node.level[0].forward
			idx++
		}
	}

	return result, nil
}

// findInsertPosition 查找插入位置（保持有序）
func (rz *RedisZSet) findInsertPosition(score float64, member []byte) int {
	entries := rz.listpack.entries
	left, right := 0, len(entries)

	for left < right {
		mid := (left + right) / 2
		if entries[mid].score < score {
			left = mid + 1
		} else if entries[mid].score > score {
			right = mid
		} else {
			// score 相同，按 member 字典序
			cmp := bytes.Compare(entries[mid].member, member)
			if cmp < 0 {
				left = mid + 1
			} else {
				right = mid
			}
		}
	}

	return left
}

// convertToSkiplist 转换为 skiplist
func (rz *RedisZSet) convertToSkiplist() {
	if rz.encoding == OBJ_ENCODING_SKIPLIST {
		return
	}

	rz.skiplist = newSkipList()
	rz.dict = make(map[string]float64)

	// 将 listpack 中的所有元素添加到 skiplist
	for _, entry := range rz.listpack.entries {
		rz.skiplist.Insert(entry.member, entry.score)
		rz.dict[string(entry.member)] = entry.score
	}

	rz.encoding = OBJ_ENCODING_SKIPLIST
	rz.listpack = nil
}

// Card 获取 ZSet 的元素数量
func (rz *RedisZSet) Card() int {
	if rz.encoding == OBJ_ENCODING_LISTPACK {
		return len(rz.listpack.entries)
	} else {
		return int(rz.skiplist.length)
	}
}

// ============================================================================
// SkipList 实现
// ============================================================================

// newSkipList 创建新的跳表
func newSkipList() *SkipList {
	sl := &SkipList{
		level:  1,
		length: 0,
		header: &SkipListNode{
			level: make([]SkipListLevel, SKIPLIST_MAXLEVEL),
		},
	}
	return sl
}

// randomLevel 随机生成层数
func randomLevel() int {
	level := 1
	for rand.Float64() < SKIPLIST_P && level < SKIPLIST_MAXLEVEL {
		level++
	}
	return level
}

// Insert 插入节点到跳表
func (sl *SkipList) Insert(member []byte, score float64) {
	update := make([]*SkipListNode, SKIPLIST_MAXLEVEL)
	rank := make([]uint32, SKIPLIST_MAXLEVEL)

	// 从顶层开始查找插入位置
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		// 向前查找
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					bytes.Compare(x.level[i].forward.member, member) < 0)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	// 生成新节点的层数
	level := randomLevel()

	// 如果新层数大于当前最大层数，更新
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	// 创建新节点
	x = &SkipListNode{
		member: member,
		score:  score,
		level:  make([]SkipListLevel, level),
	}

	// 插入节点
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		// 更新跨度
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 更新未触及层的跨度
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	// 更新后向指针
	if update[0] == sl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		sl.tail = x
	}

	sl.length++
}

// Delete 从跳表删除节点
func (sl *SkipList) Delete(member []byte, score float64) bool {
	update := make([]*SkipListNode, SKIPLIST_MAXLEVEL)

	// 查找节点
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					bytes.Compare(x.level[i].forward.member, member) < 0)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	// 找到要删除的节点
	x = x.level[0].forward
	if x != nil && x.score == score && bytes.Equal(x.member, member) {
		// 删除节点
		for i := 0; i < sl.level; i++ {
			if update[i].level[i].forward == x {
				update[i].level[i].span += x.level[i].span - 1
				update[i].level[i].forward = x.level[i].forward
			} else {
				update[i].level[i].span--
			}
		}

		// 更新后向指针
		if x.level[0].forward != nil {
			x.level[0].forward.backward = x.backward
		} else {
			sl.tail = x.backward
		}

		// 更新最大层数
		for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
			sl.level--
		}

		sl.length--
		return true
	}

	return false
}
