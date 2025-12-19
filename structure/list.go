package structure

import (
	"bytes"
	"encoding/binary"
	"errors"
)

/*
 * ============================================================================
 * Redis List 数据结构 - Quicklist + Listpack
 * ============================================================================
 *
 * 【核心原理】
 * Redis List 使用两种编码方式：
 * 1. OBJ_ENCODING_LISTPACK: 小列表使用 listpack（紧凑的序列化格式）
 * 2. OBJ_ENCODING_QUICKLIST: 大列表使用 quicklist（双向链表 + listpack）
 *
 * 【Quicklist 原理】
 * Quicklist 是一个双向链表，每个节点包含一个 listpack。
 * 这样设计的好处：
 * 1. 减少内存碎片：每个节点是一个连续的内存块
 * 2. 支持压缩：可以对节点进行 LZF 压缩
 * 3. 平衡插入/删除性能：不需要移动大量数据
 * 4. 支持分片：大列表被分成多个节点，每个节点大小可控
 *
 * 【Listpack 原理】
 * Listpack 是一个紧凑的序列化格式，用于存储多个元素。
 * 特点：
 * 1. 内存紧凑：没有额外的指针开销
 * 2. 支持字符串和整数：自动识别类型
 * 3. 支持双向遍历：可以向前和向后遍历
 * 4. 变长编码：整数和字符串都使用变长编码节省空间
 *
 * 【编码转换策略】
 * - 小列表（< 8KB 或元素数 < 512）：使用 listpack
 * - 大列表：转换为 quicklist
 * - 当 quicklist 只有一个节点且大小 < 4KB 时，可以转换回 listpack
 *
 * 【面试题】
 * Q1: Redis List 为什么使用 quicklist 而不是简单的双向链表？
 * A1: 简单双向链表的问题：
 *     - 每个节点都有 prev/next 指针，内存开销大（64位系统每个指针8字节）
 *     - 内存碎片严重（每个节点单独分配）
 *     - 缓存局部性差（节点分散在内存中）
 *     Quicklist 的优势：
 *     - 每个节点包含多个元素（listpack），减少指针开销
 *     - 节点可以压缩，节省内存
 *     - 更好的缓存局部性
 *
 * Q2: Listpack 和 Ziplist 有什么区别？
 * A2: Listpack 是 Ziplist 的改进版：
 *     - Ziplist 使用 prevlen 字段记录前一个元素的长度，导致连锁更新问题
 *     - Listpack 使用总长度字段，避免了连锁更新
 *     - Listpack 更简单，性能更好
 *
 * Q3: Quicklist 的压缩策略是什么？
 * A3: Quicklist 支持对节点进行 LZF 压缩：
 *     - compress 参数控制压缩深度（两端不压缩的节点数）
 *     - 例如 compress=1 表示两端各保留 1 个节点不压缩
 *     - 压缩可以节省内存，但会增加 CPU 开销
 *
 * Q4: 为什么 List 要支持编码转换？
 * A4: 为了在不同场景下优化性能：
 *     - 小列表：listpack 更节省内存，操作更快
 *     - 大列表：quicklist 避免大块内存分配，支持压缩
 *     根据数据规模动态选择最优编码
 *
 * Q5: Quicklist 的 fill 参数是什么？
 * A5: fill 参数控制每个节点最多包含的元素数：
 *     - fill > 0: 每个节点最多包含 fill 个元素
 *     - fill < 0: 每个节点最多包含 |fill| * 8KB 的数据
 *     这样可以控制节点大小，平衡内存和性能
 */

// ListEncoding 编码类型（使用 Encoding 别名）
// 常量定义在 encoding.go 中

const (
	LIST_MAX_LISTPACK_SIZE    = 8192 // 8KB，超过此大小转换为 quicklist
	LIST_MAX_LISTPACK_ENTRIES = 512  // 超过此元素数转换为 quicklist
	LIST_MIN_QUICKLIST_SIZE   = 4096 // 4KB，quicklist 转换回 listpack 的阈值
)

// QuicklistNode quicklist 节点
type QuicklistNode struct {
	prev      *QuicklistNode
	next      *QuicklistNode
	entry     []byte // listpack 数据
	sz        uint32 // entry 大小（字节）
	count     uint16 // listpack 中的元素数量
	encoding  uint8  // RAW=1 或 LZF=2（压缩）
	container uint8  // PLAIN=1 或 PACKED=2（listpack）
}

// Quicklist 快速列表
type Quicklist struct {
	head      *QuicklistNode
	tail      *QuicklistNode
	count     uint64 // 所有 listpack 中的总元素数
	len       uint32 // quicklistNode 的数量
	allocSize uint64 // 总分配内存（字节）
	fill      int16  // 每个节点的填充因子
	compress  uint16 // 压缩深度
}

// ListpackEntry 列表包条目
type ListpackEntry struct {
	sval  []byte // 字符串值
	lval  int64  // 整数值
	isInt bool   // 是否为整数
}

// Listpack 列表包（简化实现）
type Listpack struct {
	entries []ListpackEntry
}

// RedisList Redis List 对象
type RedisList struct {
	encoding  ListEncoding
	listpack  *Listpack  // 小列表时使用
	quicklist *Quicklist // 大列表时使用
}

// NewList 创建新的 Redis List
func NewList() *RedisList {
	return &RedisList{
		encoding: OBJ_ENCODING_LISTPACK,
		listpack: &Listpack{
			entries: make([]ListpackEntry, 0),
		},
	}
}

// Len 获取列表长度
func (rl *RedisList) Len() int {
	if rl.encoding == OBJ_ENCODING_LISTPACK {
		return len(rl.listpack.entries)
	} else {
		return int(rl.quicklist.count)
	}
}

// Push 向列表添加元素（头部或尾部）
// where: 0 = HEAD, 1 = TAIL
func (rl *RedisList) Push(value []byte, where int) {
	if rl.encoding == OBJ_ENCODING_LISTPACK {
		rl.pushListpack(value, where)
		// 检查是否需要转换为 quicklist
		rl.tryConvertToQuicklist()
	} else {
		rl.pushQuicklist(value, where)
	}
}

// pushListpack 向 listpack 添加元素
func (rl *RedisList) pushListpack(value []byte, where int) {
	entry := ListpackEntry{
		sval:  value,
		isInt: false,
	}

	if where == 0 { // HEAD
		rl.listpack.entries = append([]ListpackEntry{entry}, rl.listpack.entries...)
	} else { // TAIL
		rl.listpack.entries = append(rl.listpack.entries, entry)
	}
}

// pushQuicklist 向 quicklist 添加元素
func (rl *RedisList) pushQuicklist(value []byte, where int) {
	if rl.quicklist == nil {
		rl.quicklist = &Quicklist{
			fill:     16, // 默认每个节点最多 16 个元素
			compress: 0,  // 默认不压缩
		}
	}

	if rl.quicklist.tail == nil {
		// 创建新节点
		node := &QuicklistNode{
			entry:     make([]byte, 0),
			container: 2, // PACKED (listpack)
			encoding:  1, // RAW
		}
		rl.quicklist.head = node
		rl.quicklist.tail = node
		rl.quicklist.len = 1
	}

	// 序列化 value 到 listpack 格式
	serialized := rl.serializeListpackEntry(value)

	if where == 0 { // HEAD
		// 添加到 head 节点
		rl.quicklist.head.entry = append(serialized, rl.quicklist.head.entry...)
		rl.quicklist.head.count++
		rl.quicklist.head.sz += uint32(len(serialized))
	} else { // TAIL
		// 添加到 tail 节点
		rl.quicklist.tail.entry = append(rl.quicklist.tail.entry, serialized...)
		rl.quicklist.tail.count++
		rl.quicklist.tail.sz += uint32(len(serialized))

		// 检查节点是否超过 fill 限制
		if rl.quicklist.tail.count >= uint16(rl.quicklist.fill) {
			// 创建新节点
			newNode := &QuicklistNode{
				entry:     make([]byte, 0),
				container: 2,
				encoding:  1,
			}
			rl.quicklist.tail.next = newNode
			newNode.prev = rl.quicklist.tail
			rl.quicklist.tail = newNode
			rl.quicklist.len++
		}
	}

	rl.quicklist.count++
}

// Pop 从列表弹出元素
// where: 0 = HEAD, 1 = TAIL
func (rl *RedisList) Pop(where int) ([]byte, error) {
	if rl.encoding == OBJ_ENCODING_LISTPACK {
		return rl.popListpack(where)
	} else {
		return rl.popQuicklist(where)
	}
}

// popListpack 从 listpack 弹出元素
func (rl *RedisList) popListpack(where int) ([]byte, error) {
	if len(rl.listpack.entries) == 0 {
		return nil, errors.New("list is empty")
	}

	var entry ListpackEntry
	if where == 0 { // HEAD
		entry = rl.listpack.entries[0]
		rl.listpack.entries = rl.listpack.entries[1:]
	} else { // TAIL
		entry = rl.listpack.entries[len(rl.listpack.entries)-1]
		rl.listpack.entries = rl.listpack.entries[:len(rl.listpack.entries)-1]
	}

	return entry.sval, nil
}

// popQuicklist 从 quicklist 弹出元素
func (rl *RedisList) popQuicklist(where int) ([]byte, error) {
	if rl.quicklist == nil || rl.quicklist.count == 0 {
		return nil, errors.New("list is empty")
	}

	var node *QuicklistNode
	if where == 0 { // HEAD
		node = rl.quicklist.head
	} else { // TAIL
		node = rl.quicklist.tail
	}

	if node == nil || node.count == 0 {
		return nil, errors.New("node is empty")
	}

	// 从 listpack 中读取并删除元素
	value, err := rl.deserializeListpackEntry(node.entry, where == 0)
	if err != nil {
		return nil, err
	}

	// 简化实现：实际需要从 listpack 中删除元素
	if where == 0 {
		// 从头部删除
		// 这里简化处理，实际需要解析 listpack 格式
		if node.count > 0 {
			node.count--
			rl.quicklist.count--
		}
	} else {
		// 从尾部删除
		if node.count > 0 {
			node.count--
			rl.quicklist.count--
		}
	}

	// 如果节点为空，删除节点
	if node.count == 0 && rl.quicklist.len > 1 {
		if where == 0 {
			rl.quicklist.head = node.next
			if rl.quicklist.head != nil {
				rl.quicklist.head.prev = nil
			}
		} else {
			rl.quicklist.tail = node.prev
			if rl.quicklist.tail != nil {
				rl.quicklist.tail.next = nil
			}
		}
		rl.quicklist.len--
	}

	// 检查是否需要转换回 listpack
	rl.tryConvertToListpack()

	return value, nil
}

// tryConvertToQuicklist 尝试转换为 quicklist
func (rl *RedisList) tryConvertToQuicklist() {
	if rl.encoding != OBJ_ENCODING_LISTPACK {
		return
	}

	// 检查 listpack 大小是否超过限制
	currentSize := rl.getListpackSize()
	currentCount := len(rl.listpack.entries)

	if currentSize > LIST_MAX_LISTPACK_SIZE || currentCount > LIST_MAX_LISTPACK_ENTRIES {
		// 转换为 quicklist
		ql := &Quicklist{
			head:      nil,
			tail:      nil,
			count:     uint64(currentCount),
			len:       0,
			allocSize: uint64(currentSize),
			fill:      16,
			compress:  0,
		}

		// 序列化 listpack 数据
		serialized := rl.serializeListpack()

		// 创建节点并复制数据
		node := &QuicklistNode{
			entry:     serialized,
			container: 2, // PACKED
			count:     uint16(currentCount),
			sz:        uint32(len(serialized)),
			encoding:  1, // RAW
		}

		ql.head = node
		ql.tail = node
		ql.len = 1

		rl.quicklist = ql
		rl.encoding = OBJ_ENCODING_QUICKLIST
		rl.listpack = nil
	}
}

// tryConvertToListpack 尝试转换为 listpack
func (rl *RedisList) tryConvertToListpack() {
	if rl.encoding != OBJ_ENCODING_QUICKLIST {
		return
	}

	// 只有当 quicklist 只有一个节点时才考虑转换
	if rl.quicklist.len == 1 && rl.quicklist.head != nil {
		// 检查大小是否足够小
		if rl.quicklist.head.sz < LIST_MIN_QUICKLIST_SIZE {
			// 转换回 listpack
			lp := rl.deserializeListpack(rl.quicklist.head.entry)
			rl.encoding = OBJ_ENCODING_LISTPACK
			rl.listpack = lp
			rl.quicklist = nil
		}
	}
}

// getListpackSize 获取 listpack 的估算大小
func (rl *RedisList) getListpackSize() int {
	size := 0
	for _, entry := range rl.listpack.entries {
		size += len(entry.sval) + 8 // 简化估算：字符串长度 + 8字节元数据
	}
	return size
}

// serializeListpack 序列化 listpack（简化版）
func (rl *RedisList) serializeListpack() []byte {
	buf := new(bytes.Buffer)

	// 写入元素数量
	binary.Write(buf, binary.LittleEndian, uint32(len(rl.listpack.entries)))

	// 写入每个元素
	for _, entry := range rl.listpack.entries {
		if entry.isInt {
			// 整数：标记(1字节) + 值(8字节)
			buf.WriteByte(1) // 标记为整数
			binary.Write(buf, binary.LittleEndian, entry.lval)
		} else {
			// 字符串：标记(1字节) + 长度(4字节) + 数据
			buf.WriteByte(0) // 标记为字符串
			binary.Write(buf, binary.LittleEndian, uint32(len(entry.sval)))
			buf.Write(entry.sval)
		}
	}

	return buf.Bytes()
}

// deserializeListpack 反序列化 listpack（简化版）
func (rl *RedisList) deserializeListpack(data []byte) *Listpack {
	if len(data) < 4 {
		return &Listpack{entries: make([]ListpackEntry, 0)}
	}

	buf := bytes.NewReader(data)

	// 读取元素数量
	var count uint32
	binary.Read(buf, binary.LittleEndian, &count)

	entries := make([]ListpackEntry, 0, count)

	// 读取每个元素
	for i := uint32(0); i < count; i++ {
		var entryType byte
		binary.Read(buf, binary.LittleEndian, &entryType)

		if entryType == 1 {
			// 整数
			var lval int64
			binary.Read(buf, binary.LittleEndian, &lval)
			entries = append(entries, ListpackEntry{
				lval:  lval,
				isInt: true,
			})
		} else {
			// 字符串
			var slen uint32
			binary.Read(buf, binary.LittleEndian, &slen)
			sval := make([]byte, slen)
			buf.Read(sval)
			entries = append(entries, ListpackEntry{
				sval:  sval,
				isInt: false,
			})
		}
	}

	return &Listpack{entries: entries}
}

// serializeListpackEntry 序列化单个 listpack 条目
func (rl *RedisList) serializeListpackEntry(value []byte) []byte {
	buf := new(bytes.Buffer)

	// 尝试解析为整数
	var intVal int64
	if len(value) <= 20 { // 整数最多20位
		if err := binary.Read(bytes.NewReader(value), binary.LittleEndian, &intVal); err == nil {
			// 是整数
			buf.WriteByte(1)
			binary.Write(buf, binary.LittleEndian, intVal)
			return buf.Bytes()
		}
	}

	// 是字符串
	buf.WriteByte(0)
	binary.Write(buf, binary.LittleEndian, uint32(len(value)))
	buf.Write(value)

	return buf.Bytes()
}

// deserializeListpackEntry 反序列化单个 listpack 条目
func (rl *RedisList) deserializeListpackEntry(data []byte, fromHead bool) ([]byte, error) {
	if len(data) < 1 {
		return nil, errors.New("invalid listpack entry")
	}

	buf := bytes.NewReader(data)
	var entryType byte
	binary.Read(buf, binary.LittleEndian, &entryType)

	if entryType == 1 {
		// 整数
		var lval int64
		binary.Read(buf, binary.LittleEndian, &lval)
		// 转换为字符串（简化实现：直接转换为字节）
		result := make([]byte, 8)
		binary.LittleEndian.PutUint64(result, uint64(lval))
		// 实际应该转换为可读的字符串，这里简化处理
		return result, nil
	} else {
		// 字符串
		var slen uint32
		binary.Read(buf, binary.LittleEndian, &slen)
		sval := make([]byte, slen)
		buf.Read(sval)
		return sval, nil
	}
}

// Range 获取列表指定范围的元素
func (rl *RedisList) Range(start, end int) ([][]byte, error) {
	if rl.encoding == OBJ_ENCODING_LISTPACK {
		return rl.rangeListpack(start, end)
	} else {
		return rl.rangeQuicklist(start, end)
	}
}

// rangeListpack 从 listpack 获取范围
func (rl *RedisList) rangeListpack(start, end int) ([][]byte, error) {
	length := len(rl.listpack.entries)

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
		return [][]byte{}, nil
	}

	result := make([][]byte, 0, end-start+1)
	for i := start; i <= end; i++ {
		result = append(result, rl.listpack.entries[i].sval)
	}

	return result, nil
}

// rangeQuicklist 从 quicklist 获取范围
func (rl *RedisList) rangeQuicklist(start, end int) ([][]byte, error) {
	length := int(rl.quicklist.count)

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
		return [][]byte{}, nil
	}

	// 简化实现：遍历所有节点收集元素
	result := make([][]byte, 0, end-start+1)
	current := rl.quicklist.head
	currentIndex := 0

	for current != nil {
		for i := uint16(0); i < current.count; i++ {
			if currentIndex >= start && currentIndex <= end {
				// 从 listpack 中读取元素（简化实现）
				entry, _ := rl.deserializeListpackEntry(current.entry, true)
				result = append(result, entry)
			}
			currentIndex++
			if currentIndex > end {
				return result, nil
			}
		}
		current = current.next
	}

	return result, nil
}
