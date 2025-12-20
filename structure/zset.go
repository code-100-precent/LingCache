package structure

import (
	"bytes"
	"errors"
	"math/rand"
	"strconv"
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
	listpack *ListpackFull      // 小集合使用 ListpackFull（存储 member-score 对，按 score 排序）
	skiplist *SkipList          // 大集合使用
	dict     map[string]float64 // member -> score 映射（简化实现）
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
		listpack: NewListpackFull(256),
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
	if rz.listpack == nil {
		rz.listpack = NewListpackFull(256)
	}

	// 检查是否需要转换（listpack 中 member-score 对算作 2 个元素）
	if rz.listpack.Length()/2 >= ZSET_MAX_LISTPACK_ENTRIES ||
		len(member) > ZSET_MAX_LISTPACK_VALUE {
		rz.convertToSkiplist()
		return rz.addSkiplist(member, score)
	}

	// 查找插入位置（保持有序）
	insertIdx := rz.findInsertPositionInListpack(score, member)

	// 检查是否已存在
	if insertIdx >= 0 && insertIdx < int(rz.listpack.Length()/2) {
		// 检查该位置的 member 是否匹配
		if rz.getMemberAt(insertIdx) != nil && bytes.Equal(rz.getMemberAt(insertIdx), member) {
			// 更新 score
			rz.updateScoreAt(insertIdx, score)
			return nil
		}
	}

	// 插入新元素（需要重建 listpack）
	rz.insertAtPosition(insertIdx, member, score)

	return nil
}

// findInsertPositionInListpack 查找插入位置（返回 member 的索引位置）
func (rz *RedisZSet) findInsertPositionInListpack(score float64, member []byte) int {
	if rz.listpack == nil || rz.listpack.Length() == 0 {
		return 0
	}

	// 二分查找
	left, right := 0, int(rz.listpack.Length()/2)

	for left < right {
		mid := (left + right) / 2
		midScore := rz.getScoreAt(mid)

		if midScore < score {
			left = mid + 1
		} else if midScore > score {
			right = mid
		} else {
			// score 相同，按 member 字典序
			midMember := rz.getMemberAt(mid)
			cmp := bytes.Compare(midMember, member)
			if cmp < 0 {
				left = mid + 1
			} else {
				right = mid
			}
		}
	}

	return left
}

// getMemberAt 获取指定位置的 member
func (rz *RedisZSet) getMemberAt(idx int) []byte {
	if rz.listpack == nil {
		return nil
	}

	p := rz.listpack.First()
	currentIdx := 0

	for p != nil && currentIdx < idx*2 {
		var err error
		p, err = rz.listpack.Next(p)
		if err != nil || p == nil {
			return nil
		}
		currentIdx++
	}

	if p == nil {
		return nil
	}

	sval, _, isInt, err := rz.listpack.GetValue(p)
	if err != nil || isInt {
		return nil
	}

	return sval
}

// getScoreAt 获取指定位置的 score
func (rz *RedisZSet) getScoreAt(idx int) float64 {
	if rz.listpack == nil {
		return 0
	}

	// score 在 member 之后（idx*2+1 的位置）
	p := rz.listpack.First()
	currentIdx := 0

	for p != nil && currentIdx < idx*2+1 {
		var err error
		p, err = rz.listpack.Next(p)
		if err != nil || p == nil {
			return 0
		}
		currentIdx++
	}

	if p == nil {
		return 0
	}

	sval, ival, isInt, err := rz.listpack.GetValue(p)
	if err != nil {
		return 0
	}

	// 解析 score（存储为字符串）
	if isInt {
		return float64(ival)
	}
	score, err := strconv.ParseFloat(string(sval), 64)
	if err != nil {
		return 0
	}
	return score
}

// updateScoreAt 更新指定位置的 score
func (rz *RedisZSet) updateScoreAt(idx int, newScore float64) {
	// 收集所有元素
	entries := make([]ZSetEntry, 0, rz.listpack.Length()/2)

	p := rz.listpack.First()
	currentIdx := 0
	var currentMember []byte

	for p != nil {
		sval, _, _, err := rz.listpack.GetValue(p)
		if err != nil {
			break
		}

		if currentIdx%2 == 0 {
			// member
			currentMember = sval
		} else {
			// score
			score := rz.parseScore(sval)
			if currentIdx/2 == idx {
				// 更新这个位置的 score
				entries = append(entries, ZSetEntry{
					member: currentMember,
					score:  newScore,
				})
			} else {
				entries = append(entries, ZSetEntry{
					member: currentMember,
					score:  score,
				})
			}
		}

		var nextErr error
		p, nextErr = rz.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		currentIdx++
	}

	// 重建 listpack
	rz.listpack = NewListpackFull(256)
	for _, entry := range entries {
		rz.listpack.AppendString(entry.member)
		rz.listpack.AppendString(rz.scoreToBytes(entry.score))
	}
}

// insertAtPosition 在指定位置插入元素
func (rz *RedisZSet) insertAtPosition(idx int, member []byte, score float64) {
	// 收集所有元素
	entries := make([]ZSetEntry, 0, rz.listpack.Length()/2+1)

	p := rz.listpack.First()
	currentIdx := 0
	var currentMember []byte

	for p != nil {
		sval, _, _, err := rz.listpack.GetValue(p)
		if err != nil {
			break
		}

		if currentIdx%2 == 0 {
			// member
			currentMember = sval
		} else {
			// score
			score := rz.parseScore(sval)
			entries = append(entries, ZSetEntry{
				member: currentMember,
				score:  score,
			})
		}

		var nextErr error
		p, nextErr = rz.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		currentIdx++
	}

	// 插入新元素
	newEntry := ZSetEntry{member: member, score: score}
	entries = append(entries, ZSetEntry{})
	copy(entries[idx+1:], entries[idx:])
	entries[idx] = newEntry

	// 重建 listpack
	rz.listpack = NewListpackFull(256)
	for _, entry := range entries {
		rz.listpack.AppendString(entry.member)
		rz.listpack.AppendString(rz.scoreToBytes(entry.score))
	}
}

// parseScore 解析 score 字符串
func (rz *RedisZSet) parseScore(data []byte) float64 {
	score, err := strconv.ParseFloat(string(data), 64)
	if err != nil {
		return 0
	}
	return score
}

// scoreToBytes 将 score 转换为字节数组
func (rz *RedisZSet) scoreToBytes(score float64) []byte {
	return []byte(strconv.FormatFloat(score, 'f', -1, 64))
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
	if rz.listpack == nil {
		return errors.New("member not found")
	}

	// 查找 member 的位置
	memberIdx := -1
	p := rz.listpack.First()
	idx := 0

	for p != nil {
		sval, _, isInt, err := rz.listpack.GetValue(p)
		if err != nil {
			break
		}

		// 只检查 member（偶数索引）
		if idx%2 == 0 && !isInt && bytes.Equal(sval, member) {
			memberIdx = idx / 2
			break
		}

		var nextErr error
		p, nextErr = rz.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
	}

	if memberIdx < 0 {
		return errors.New("member not found")
	}

	// 收集所有元素，跳过要删除的
	entries := make([]ZSetEntry, 0, rz.listpack.Length()/2-1)

	p = rz.listpack.First()
	idx = 0
	var currentMember []byte

	for p != nil {
		sval, _, _, err := rz.listpack.GetValue(p)
		if err != nil {
			break
		}

		if idx%2 == 0 {
			// member
			if idx/2 == memberIdx {
				// 跳过这个 member 和它的 score
				var nextErr error
				p, nextErr = rz.listpack.Next(p)
				if nextErr != nil || p == nil {
					break
				}
				idx++ // 跳过 score
				continue
			}
			currentMember = sval
		} else {
			// score
			score := rz.parseScore(sval)
			entries = append(entries, ZSetEntry{
				member: currentMember,
				score:  score,
			})
		}

		var nextErr error
		p, nextErr = rz.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
	}

	// 重建 listpack
	rz.listpack = NewListpackFull(256)
	for _, entry := range entries {
		rz.listpack.AppendString(entry.member)
		rz.listpack.AppendString(rz.scoreToBytes(entry.score))
	}

	return nil
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
	if rz.listpack == nil {
		return 0, false
	}

	p := rz.listpack.First()
	idx := 0
	var currentMember []byte

	for p != nil {
		sval, _, _, err := rz.listpack.GetValue(p)
		if err != nil {
			break
		}

		if idx%2 == 0 {
			// member
			currentMember = sval
		} else {
			// score
			if bytes.Equal(currentMember, member) {
				return rz.parseScore(sval), true
			}
		}

		var nextErr error
		p, nextErr = rz.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
		idx++
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
	if rz.listpack == nil {
		return 0, false
	}

	p := rz.listpack.First()
	idx := 0
	rank := 0

	for p != nil {
		sval, _, _, err := rz.listpack.GetValue(p)
		if err != nil {
			break
		}

		// 只检查 member（偶数索引）
		if idx%2 == 0 && bytes.Equal(sval, member) {
			if reverse {
				return int(rz.listpack.Length()/2) - 1 - rank, true
			}
			return rank, true
		}

		if idx%2 == 1 {
			// 完成一个 member-score 对
			rank++
		}

		var nextErr error
		p, nextErr = rz.listpack.Next(p)
		if nextErr != nil || p == nil {
			break
		}
		idx++
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
	if rz.listpack == nil {
		return []ZSetEntry{}, nil
	}

	length := int(rz.listpack.Length() / 2)

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
		entries := make([]ZSetEntry, 0, length)
		p := rz.listpack.First()
		idx := 0
		var currentMember []byte

		for p != nil {
			sval, _, _, err := rz.listpack.GetValue(p)
			if err != nil {
				break
			}

			if idx%2 == 0 {
				currentMember = sval
			} else {
				score := rz.parseScore(sval)
				entries = append(entries, ZSetEntry{
					member: currentMember,
					score:  score,
				})
			}

			var nextErr error
			p, nextErr = rz.listpack.Next(p)
			if nextErr != nil || p == nil {
				break
			}
			idx++
		}

		for i := end; i >= start; i-- {
			if i < len(entries) {
				result = append(result, entries[i])
			}
		}
	} else {
		// 正向遍历
		p := rz.listpack.First()
		idx := 0
		rank := 0
		var currentMember []byte

		for p != nil && rank <= end {
			sval, _, _, err := rz.listpack.GetValue(p)
			if err != nil {
				break
			}

			if idx%2 == 0 {
				currentMember = sval
			} else {
				score := rz.parseScore(sval)
				if rank >= start {
					result = append(result, ZSetEntry{
						member: currentMember,
						score:  score,
					})
				}
				rank++
			}

			var nextErr error
			p, nextErr = rz.listpack.Next(p)
			if nextErr != nil || p == nil {
				break
			}
			idx++
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

// convertToSkiplist 转换为 skiplist
func (rz *RedisZSet) convertToSkiplist() {
	if rz.encoding == OBJ_ENCODING_SKIPLIST {
		return
	}

	rz.skiplist = newSkipList()
	rz.dict = make(map[string]float64)

	// 将 listpack 中的所有元素添加到 skiplist
	if rz.listpack != nil {
		p := rz.listpack.First()
		idx := 0
		var currentMember []byte

		for p != nil {
			sval, _, _, err := rz.listpack.GetValue(p)
			if err != nil {
				break
			}

			if idx%2 == 0 {
				// member
				currentMember = sval
			} else {
				// score
				score := rz.parseScore(sval)
				rz.skiplist.Insert(currentMember, score)
				rz.dict[string(currentMember)] = score
			}

			var nextErr error
			p, nextErr = rz.listpack.Next(p)
			if nextErr != nil || p == nil {
				break
			}
			idx++
		}
	}

	rz.encoding = OBJ_ENCODING_SKIPLIST
	rz.listpack = nil
}

// Card 获取 ZSet 的元素数量
func (rz *RedisZSet) Card() int {
	if rz.encoding == OBJ_ENCODING_LISTPACK {
		if rz.listpack == nil {
			return 0
		}
		// listpack 中 member-score 成对存储，所以长度除以 2
		return int(rz.listpack.Length() / 2)
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
