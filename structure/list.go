package structure

import (
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
	entry     []byte        // listpack 二进制数据（使用 ListpackFull 生成）
	sz        uint32        // entry 大小（字节）
	count     uint16        // listpack 中的元素数量
	encoding  uint8         // RAW=1 或 LZF=2（压缩）
	container uint8         // PLAIN=1 或 PACKED=2（listpack）
	listpack  *ListpackFull // 内部使用的 ListpackFull 对象（用于操作）
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

// RedisList Redis List 对象
type RedisList struct {
	encoding  ListEncoding
	listpack  *ListpackFull // 小列表时使用 ListpackFull
	quicklist *Quicklist    // 大列表时使用 quicklist
}

// NewList 创建新的 Redis List
func NewList() *RedisList {
	return &RedisList{
		encoding: OBJ_ENCODING_LISTPACK,
		listpack: NewListpackFull(256), // 初始容量 256 字节
	}
}

// Len 获取列表长度
func (rl *RedisList) Len() int {
	if rl.encoding == OBJ_ENCODING_LISTPACK {
		return int(rl.listpack.Length())
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
	if rl.listpack == nil {
		rl.listpack = NewListpackFull(256)
	}

	// 尝试解析为整数
	if intVal, ok := rl.tryParseInt(value); ok {
		// 如果是整数，使用 AppendInteger
		if where == 0 { // HEAD - listpack 不支持头部插入，需要重建
			rl.insertAtHead(value, intVal)
		} else { // TAIL
			rl.listpack.AppendInteger(intVal)
		}
	} else {
		// 字符串
		if where == 0 { // HEAD - listpack 不支持头部插入，需要重建
			rl.insertAtHead(value, 0)
		} else { // TAIL
			rl.listpack.AppendString(value)
		}
	}
}

// insertAtHead 在头部插入元素（需要重建 listpack）
func (rl *RedisList) insertAtHead(value []byte, intVal int64) {
	// 收集所有现有元素
	oldEntries := make([][]byte, 0, rl.listpack.Length())
	oldInts := make([]int64, 0, rl.listpack.Length())

	p := rl.listpack.First()
	idx := 0
	for p != nil {
		sval, ival, isInt, _ := rl.listpack.GetValue(p)
		if isInt {
			oldInts = append(oldInts, ival)
			oldEntries = append(oldEntries, nil) // 标记为整数
		} else {
			oldEntries = append(oldEntries, sval)
		}
		var err error
		p, err = rl.listpack.Next(p)
		if err != nil || p == nil {
			break
		}
		idx++
	}

	// 重建 listpack，先插入新元素
	rl.listpack = NewListpackFull(256)
	if intVal != 0 {
		rl.listpack.AppendInteger(intVal)
	} else {
		rl.listpack.AppendString(value)
	}

	// 再添加旧元素
	intIdx := 0
	for _, entry := range oldEntries {
		if entry == nil {
			// 整数
			if intIdx < len(oldInts) {
				rl.listpack.AppendInteger(oldInts[intIdx])
				intIdx++
			}
		} else {
			rl.listpack.AppendString(entry)
		}
	}
}

// tryParseInt 尝试解析为整数
func (rl *RedisList) tryParseInt(value []byte) (int64, bool) {
	if len(value) == 0 {
		return 0, false
	}

	negative := false
	start := 0
	if value[0] == '-' {
		negative = true
		start = 1
	} else if value[0] == '+' {
		start = 1
	}

	if start >= len(value) {
		return 0, false
	}

	// 检查是否全是数字
	for i := start; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, false
		}
	}

	// 解析整数
	var val int64
	for i := start; i < len(value); i++ {
		val = val*10 + int64(value[i]-'0')
	}

	if negative {
		val = -val
	}

	return val, true
}

// pushQuicklist 向 quicklist 添加元素
func (rl *RedisList) pushQuicklist(value []byte, where int) {
	if rl.quicklist == nil {
		rl.quicklist = &Quicklist{
			fill:     16, // 默认每个节点最多 16 个元素
			compress: 0,  // 默认不压缩
		}
	}

	var targetNode *QuicklistNode
	if where == 0 { // HEAD
		targetNode = rl.quicklist.head
	} else { // TAIL
		targetNode = rl.quicklist.tail
	}

	// 如果节点不存在，创建新节点
	if targetNode == nil {
		node := rl.newQuicklistNode()
		rl.quicklist.head = node
		rl.quicklist.tail = node
		rl.quicklist.len = 1
		targetNode = node
	}

	// 如果节点的 listpack 为空，初始化
	if targetNode.listpack == nil {
		targetNode.listpack = NewListpackFull(256)
	}

	// 尝试解析为整数
	if intVal, ok := rl.tryParseInt(value); ok {
		if where == 0 {
			// HEAD - listpack 不支持头部插入，需要重建或使用新节点
			rl.insertAtQuicklistHead(targetNode, value, intVal)
		} else {
			targetNode.listpack.AppendInteger(intVal)
		}
	} else {
		if where == 0 {
			rl.insertAtQuicklistHead(targetNode, value, 0)
		} else {
			targetNode.listpack.AppendString(value)
		}
	}

	// 更新节点信息
	targetNode.entry = targetNode.listpack.Bytes()
	targetNode.sz = uint32(len(targetNode.entry))
	targetNode.count = targetNode.listpack.Length()

	// 检查是否需要创建新节点（尾部插入时）
	if where == 1 && targetNode.count >= uint16(rl.quicklist.fill) {
		newNode := rl.newQuicklistNode()
		rl.quicklist.tail.next = newNode
		newNode.prev = rl.quicklist.tail
		rl.quicklist.tail = newNode
		rl.quicklist.len++
	}

	rl.quicklist.count++
}

// newQuicklistNode 创建新的 quicklist 节点
func (rl *RedisList) newQuicklistNode() *QuicklistNode {
	return &QuicklistNode{
		entry:     make([]byte, 0),
		container: 2, // PACKED (listpack)
		encoding:  1, // RAW
		listpack:  NewListpackFull(256),
	}
}

// insertAtQuicklistHead 在 quicklist 节点头部插入（需要重建 listpack）
func (rl *RedisList) insertAtQuicklistHead(node *QuicklistNode, value []byte, intVal int64) {
	if node.listpack == nil {
		node.listpack = NewListpackFull(256)
	}

	// 收集所有现有元素
	oldEntries := make([][]byte, 0, node.listpack.Length())
	oldInts := make([]int64, 0, node.listpack.Length())

	p := node.listpack.First()
	for p != nil {
		sval, ival, isInt, _ := node.listpack.GetValue(p)
		if isInt {
			oldInts = append(oldInts, ival)
			oldEntries = append(oldEntries, nil) // 标记为整数
		} else {
			oldEntries = append(oldEntries, sval)
		}
		var err error
		p, err = node.listpack.Next(p)
		if err != nil || p == nil {
			break
		}
	}

	// 重建 listpack，先插入新元素
	node.listpack = NewListpackFull(256)
	if intVal != 0 {
		node.listpack.AppendInteger(intVal)
	} else {
		node.listpack.AppendString(value)
	}

	// 再添加旧元素
	for i, entry := range oldEntries {
		if entry == nil {
			node.listpack.AppendInteger(oldInts[i])
		} else {
			node.listpack.AppendString(entry)
		}
	}
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
	if rl.listpack == nil || rl.listpack.Length() == 0 {
		return nil, errors.New("list is empty")
	}

	// listpack 不支持直接删除，需要重建
	// 获取要删除的元素
	var value []byte
	var intVal int64
	var isInt bool

	if where == 0 { // HEAD
		p := rl.listpack.First()
		if p == nil {
			return nil, errors.New("list is empty")
		}
		var err error
		value, intVal, isInt, err = rl.listpack.GetValue(p)
		if err != nil {
			return nil, err
		}
	} else { // TAIL
		// 找到最后一个元素
		p := rl.listpack.First()
		var lastP []byte
		for p != nil {
			lastP = p
			var err error
			p, err = rl.listpack.Next(p)
			if err != nil || p == nil {
				break
			}
		}
		if lastP == nil {
			return nil, errors.New("list is empty")
		}
		var err error
		value, intVal, isInt, err = rl.listpack.GetValue(lastP)
		if err != nil {
			return nil, err
		}
	}

	// 重建 listpack，跳过要删除的元素
	oldEntries := make([][]byte, 0, rl.listpack.Length()-1)
	oldInts := make([]int64, 0, rl.listpack.Length()-1)

	p := rl.listpack.First()
	idx := 0
	for p != nil {
		sval, ival, entryIsInt, _ := rl.listpack.GetValue(p)

		// 检查是否是要删除的元素
		shouldSkip := false
		if where == 0 {
			// HEAD - 跳过第一个（通过索引判断）
			if idx == 0 {
				shouldSkip = true
			}
		} else {
			// TAIL - 跳过最后一个
			next, _ := rl.listpack.Next(p)
			if next == nil {
				shouldSkip = true
			}
		}

		if !shouldSkip {
			if entryIsInt {
				oldInts = append(oldInts, ival)
				oldEntries = append(oldEntries, nil)
			} else {
				oldEntries = append(oldEntries, sval)
			}
		}

		var err error
		p, err = rl.listpack.Next(p)
		if err != nil || p == nil {
			break
		}
	}

	// 重建 listpack
	rl.listpack = NewListpackFull(256)
	for i, entry := range oldEntries {
		if entry == nil {
			rl.listpack.AppendInteger(oldInts[i])
		} else {
			rl.listpack.AppendString(entry)
		}
	}

	// 返回删除的值
	if isInt {
		// 将整数转换为字符串
		return rl.intToBytes(intVal), nil
	}
	return value, nil
}

// intToBytes 将整数转换为字节数组
func (rl *RedisList) intToBytes(val int64) []byte {
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

	// 确保 listpack 对象存在
	if node.listpack == nil {
		// 从 entry 重建 listpack
		node.listpack = NewListpackFull(256)
		// 这里需要从 entry 解析，简化处理：直接使用 popListpack 的逻辑
		// 实际应该解析 entry 的二进制数据
		return nil, errors.New("node listpack not initialized")
	}

	// 使用 listpack 的方法删除元素（需要重建）
	var value []byte
	var intVal int64
	var isInt bool

	if where == 0 { // HEAD
		p := node.listpack.First()
		if p == nil {
			return nil, errors.New("node is empty")
		}
		var err error
		value, intVal, isInt, err = node.listpack.GetValue(p)
		if err != nil {
			return nil, err
		}
	} else { // TAIL
		// 找到最后一个元素
		p := node.listpack.First()
		var lastP []byte
		for p != nil {
			lastP = p
			var err error
			p, err = node.listpack.Next(p)
			if err != nil || p == nil {
				break
			}
		}
		if lastP == nil {
			return nil, errors.New("node is empty")
		}
		var err error
		value, intVal, isInt, err = node.listpack.GetValue(lastP)
		if err != nil {
			return nil, err
		}
	}

	// 重建 listpack，跳过要删除的元素
	oldEntries := make([][]byte, 0, node.listpack.Length()-1)
	oldInts := make([]int64, 0, node.listpack.Length()-1)

	p := node.listpack.First()
	idx := 0
	for p != nil {
		sval, ival, entryIsInt, _ := node.listpack.GetValue(p)

		// 检查是否是要删除的元素
		shouldSkip := false
		if where == 0 {
			// HEAD - 跳过第一个（通过索引判断）
			if idx == 0 {
				shouldSkip = true
			}
		} else {
			// TAIL - 跳过最后一个
			next, _ := node.listpack.Next(p)
			if next == nil {
				shouldSkip = true
			}
		}

		if !shouldSkip {
			if entryIsInt {
				oldInts = append(oldInts, ival)
				oldEntries = append(oldEntries, nil)
			} else {
				oldEntries = append(oldEntries, sval)
			}
		}

		var err error
		p, err = node.listpack.Next(p)
		if err != nil || p == nil {
			break
		}
		idx++
	}

	// 重建 listpack
	node.listpack = NewListpackFull(256)
	for i, entry := range oldEntries {
		if entry == nil {
			node.listpack.AppendInteger(oldInts[i])
		} else {
			node.listpack.AppendString(entry)
		}
	}

	// 更新节点信息
	node.entry = node.listpack.Bytes()
	node.sz = uint32(len(node.entry))
	node.count = node.listpack.Length()
	rl.quicklist.count--

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

	// 返回删除的值
	if isInt {
		return rl.intToBytes(intVal), nil
	}
	return value, nil
}

// tryConvertToQuicklist 尝试转换为 quicklist
func (rl *RedisList) tryConvertToQuicklist() {
	if rl.encoding != OBJ_ENCODING_LISTPACK {
		return
	}

	if rl.listpack == nil {
		return
	}

	// 检查 listpack 大小是否超过限制
	currentSize := len(rl.listpack.Bytes())
	currentCount := int(rl.listpack.Length())

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

		// 创建节点并复制 listpack 数据
		node := &QuicklistNode{
			entry:     rl.listpack.Bytes(),
			container: 2, // PACKED
			count:     rl.listpack.Length(),
			sz:        uint32(currentSize),
			encoding:  1,           // RAW
			listpack:  rl.listpack, // 保留引用
		}

		ql.head = node
		ql.tail = node
		ql.len = 1

		rl.quicklist = ql
		rl.encoding = OBJ_ENCODING_QUICKLIST
		rl.listpack = nil // 不再直接使用，由 quicklist 节点持有
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
			if rl.quicklist.head.listpack != nil {
				rl.listpack = rl.quicklist.head.listpack
			} else {
				// 从 entry 重建 listpack（简化处理）
				rl.listpack = NewListpackFull(256)
				// 实际应该解析 entry 的二进制数据
			}
			rl.encoding = OBJ_ENCODING_LISTPACK
			rl.quicklist = nil
		}
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
	if rl.listpack == nil {
		return [][]byte{}, nil
	}

	length := int(rl.listpack.Length())

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
	p := rl.listpack.First()
	idx := 0

	for p != nil && idx <= end {
		if idx >= start {
			sval, ival, isInt, err := rl.listpack.GetValue(p)
			if err != nil {
				return nil, err
			}
			if isInt {
				result = append(result, rl.intToBytes(ival))
			} else {
				result = append(result, sval)
			}
		}
		var err error
		p, err = rl.listpack.Next(p)
		if err != nil || p == nil {
			break
		}
		idx++
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

	result := make([][]byte, 0, end-start+1)
	current := rl.quicklist.head
	currentIndex := 0

	for current != nil && currentIndex <= end {
		// 确保 listpack 对象存在
		if current.listpack == nil {
			// 从 entry 重建（简化处理）
			current.listpack = NewListpackFull(256)
			// 实际应该解析 entry 的二进制数据
			current = current.next
			continue
		}

		// 遍历当前节点的 listpack
		p := current.listpack.First()
		for p != nil && currentIndex <= end {
			if currentIndex >= start {
				sval, ival, isInt, err := current.listpack.GetValue(p)
				if err != nil {
					return nil, err
				}
				if isInt {
					result = append(result, rl.intToBytes(ival))
				} else {
					result = append(result, sval)
				}
			}
			var err error
			p, err = current.listpack.Next(p)
			if err != nil || p == nil {
				break
			}
			currentIndex++
		}

		current = current.next
	}

	return result, nil
}
