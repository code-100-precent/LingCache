package structure

import (
	"encoding/binary"
	"errors"
)

/*
 * ============================================================================
 * Listpack 完整实现
 * ============================================================================
 *
 * Listpack 是 Redis 7.0 引入的紧凑序列化格式，用于替代 ziplist。
 *
 * 【内存布局】
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 * | Total  | Num    | Entry1 | Entry2 | ...    | EntryN | Backlen| 0xFF   |
 * | Bytes  | Elem   |        |        |        |        |        | (EOF)  |
 * | (4B)   | (2B)   |        |        |        |        |        |        |
 * +--------+--------+--------+--------+--------+--------+--------+--------+
 *
 * 【编码格式】
 * - 7-bit 整数: 0xxxxxxx (0-127)
 * - 6-bit 字符串: 10xxxxxx (长度在编码字节中)
 * - 13-bit 整数: 110xxxxx xxxxxxxx (-4096 到 4095)
 * - 12-bit 字符串: 1110xxxx xxxxxxxx (长度在2字节中)
 * - 16-bit 整数: 11110001 xxxxxxxx xxxxxxxx
 * - 24-bit 整数: 11110010 xxxxxxxx xxxxxxxx xxxxxxxx
 * - 32-bit 整数: 11110011 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 * - 64-bit 整数: 11110100 xxxxxxxx ... (9字节)
 * - 32-bit 字符串: 11110000 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx (5字节长度)
 *
 * 【Backlen】
 * 每个元素后面都有一个反向编码的长度字段（backlen），用于向前遍历。
 * 使用变长编码，1-5 字节。
 */

const (
	LP_HDR_SIZE = 6 // 4字节总长度 + 2字节元素数量
	LP_EOF      = 0xFF

	// 编码类型
	LP_ENCODING_7BIT_UINT = 0x00
	LP_ENCODING_6BIT_STR  = 0x80
	LP_ENCODING_13BIT_INT = 0xC0
	LP_ENCODING_12BIT_STR = 0xE0
	LP_ENCODING_16BIT_INT = 0xF1
	LP_ENCODING_24BIT_INT = 0xF2
	LP_ENCODING_32BIT_INT = 0xF3
	LP_ENCODING_64BIT_INT = 0xF4
	LP_ENCODING_32BIT_STR = 0xF0

	// 掩码
	LP_ENCODING_7BIT_UINT_MASK = 0x80
	LP_ENCODING_6BIT_STR_MASK  = 0xC0
	LP_ENCODING_13BIT_INT_MASK = 0xE0
	LP_ENCODING_12BIT_STR_MASK = 0xF0
)

// ListpackFull 完整的 listpack 实现
type ListpackFull struct {
	data []byte // 二进制数据
}

// NewListpackFull 创建新的 listpack
func NewListpackFull(capacity int) *ListpackFull {
	if capacity < LP_HDR_SIZE+1 {
		capacity = LP_HDR_SIZE + 1
	}

	lp := &ListpackFull{
		data: make([]byte, capacity),
	}

	// 初始化 header
	lp.setTotalBytes(LP_HDR_SIZE + 1)
	lp.setNumElements(0)
	lp.data[LP_HDR_SIZE] = LP_EOF

	return lp
}

// setTotalBytes 设置总字节数
func (lp *ListpackFull) setTotalBytes(v uint32) {
	lp.data[0] = byte(v)
	lp.data[1] = byte(v >> 8)
	lp.data[2] = byte(v >> 16)
	lp.data[3] = byte(v >> 24)
}

// getTotalBytes 获取总字节数
func (lp *ListpackFull) getTotalBytes() uint32 {
	return uint32(lp.data[0]) |
		uint32(lp.data[1])<<8 |
		uint32(lp.data[2])<<16 |
		uint32(lp.data[3])<<24
}

// setNumElements 设置元素数量
func (lp *ListpackFull) setNumElements(v uint16) {
	lp.data[4] = byte(v)
	lp.data[5] = byte(v >> 8)
}

// getNumElements 获取元素数量
func (lp *ListpackFull) getNumElements() uint16 {
	return uint16(lp.data[4]) | uint16(lp.data[5])<<8
}

// Bytes 获取 listpack 的二进制数据
func (lp *ListpackFull) Bytes() []byte {
	totalBytes := lp.getTotalBytes()
	return lp.data[:totalBytes]
}

// Length 获取元素数量
func (lp *ListpackFull) Length() uint16 {
	return lp.getNumElements()
}

// Append 追加字符串元素
func (lp *ListpackFull) Append(s []byte) error {
	return lp.AppendString(s)
}

// AppendString 追加字符串元素
func (lp *ListpackFull) AppendString(s []byte) error {
	entryLen := lp.encodeStringSize(len(s))
	backlenSize := lp.encodeBacklenSize(uint64(entryLen))

	totalBytes := lp.getTotalBytes()
	newTotalBytes := totalBytes + uint32(entryLen) + uint32(backlenSize)

	// 检查是否需要扩容
	if newTotalBytes > uint32(len(lp.data)) {
		lp.grow(newTotalBytes)
	}

	// 编码字符串
	entryStart := int(totalBytes)
	lp.encodeString(lp.data[entryStart:], s)

	// 编码 backlen
	backlenStart := entryStart + entryLen
	lp.encodeBacklen(lp.data[backlenStart:], uint64(entryLen))

	// 更新总长度和元素数量
	lp.setTotalBytes(newTotalBytes)
	lp.setNumElements(lp.getNumElements() + 1)

	// 设置新的 EOF
	lp.data[newTotalBytes-1] = LP_EOF

	return nil
}

// AppendInteger 追加整数元素
func (lp *ListpackFull) AppendInteger(v int64) error {
	entryLen := lp.encodeIntegerSize(v)
	backlenSize := lp.encodeBacklenSize(uint64(entryLen))

	totalBytes := lp.getTotalBytes()
	newTotalBytes := totalBytes + uint32(entryLen) + uint32(backlenSize)

	// 检查是否需要扩容
	if newTotalBytes > uint32(len(lp.data)) {
		lp.grow(newTotalBytes)
	}

	// 编码整数
	entryStart := int(totalBytes)
	lp.encodeInteger(lp.data[entryStart:], v)

	// 编码 backlen
	backlenStart := entryStart + entryLen
	lp.encodeBacklen(lp.data[backlenStart:], uint64(entryLen))

	// 更新总长度和元素数量
	lp.setTotalBytes(newTotalBytes)
	lp.setNumElements(lp.getNumElements() + 1)

	// 设置新的 EOF
	lp.data[newTotalBytes-1] = LP_EOF

	return nil
}

// Get 获取指定索引的元素
func (lp *ListpackFull) Get(index int) ([]byte, int64, bool, error) {
	if index < 0 || index >= int(lp.getNumElements()) {
		return nil, 0, false, errors.New("index out of range")
	}

	p := lp.First()
	for i := 0; i < index; i++ {
		var err error
		p, err = lp.Next(p)
		if err != nil {
			return nil, 0, false, err
		}
	}

	return lp.GetValue(p)
}

// GetValue 获取指针指向的元素值
func (lp *ListpackFull) GetValue(p []byte) ([]byte, int64, bool, error) {
	if len(p) == 0 {
		return nil, 0, false, errors.New("invalid pointer")
	}

	encoding := p[0]

	// 检查编码类型
	if encoding&LP_ENCODING_7BIT_UINT_MASK == 0 {
		// 7-bit 整数
		return nil, int64(encoding), true, nil
	}

	if encoding&LP_ENCODING_6BIT_STR_MASK == LP_ENCODING_6BIT_STR {
		// 6-bit 字符串
		strLen := int(encoding & 0x3F)
		if strLen+1 > len(p) {
			return nil, 0, false, errors.New("invalid 6-bit string")
		}
		val := make([]byte, strLen)
		copy(val, p[1:1+strLen])
		return val, 0, false, nil
	}

	if encoding&LP_ENCODING_13BIT_INT_MASK == LP_ENCODING_13BIT_INT {
		// 13-bit 整数
		if len(p) < 2 {
			return nil, 0, false, errors.New("invalid 13-bit int")
		}
		val := int64((int(encoding)&0x1F)<<8 | int(p[1]))
		if val >= 4096 {
			val -= 8192
		}
		return nil, val, true, nil
	}

	if encoding&LP_ENCODING_12BIT_STR_MASK == LP_ENCODING_12BIT_STR {
		// 12-bit 字符串
		if len(p) < 2 {
			return nil, 0, false, errors.New("invalid 12-bit string")
		}
		strLen := int((int(encoding)&0xF)<<8 | int(p[1]))
		if strLen+2 > len(p) {
			return nil, 0, false, errors.New("invalid 12-bit string")
		}
		val := make([]byte, strLen)
		copy(val, p[2:2+strLen])
		return val, 0, false, nil
	}

	if encoding == LP_ENCODING_16BIT_INT {
		// 16-bit 整数
		if len(p) < 3 {
			return nil, 0, false, errors.New("invalid 16-bit int")
		}
		val := int64(int16(binary.LittleEndian.Uint16(p[1:3])))
		return nil, val, true, nil
	}

	if encoding == LP_ENCODING_24BIT_INT {
		// 24-bit 整数
		if len(p) < 4 {
			return nil, 0, false, errors.New("invalid 24-bit int")
		}
		val := int64(int32(p[1] | p[2]<<8 | p[3]<<16))
		if val >= 8388608 {
			val -= 16777216
		}
		return nil, val, true, nil
	}

	if encoding == LP_ENCODING_32BIT_INT {
		// 32-bit 整数
		if len(p) < 5 {
			return nil, 0, false, errors.New("invalid 32-bit int")
		}
		val := int64(int32(binary.LittleEndian.Uint32(p[1:5])))
		return nil, val, true, nil
	}

	if encoding == LP_ENCODING_64BIT_INT {
		// 64-bit 整数
		if len(p) < 9 {
			return nil, 0, false, errors.New("invalid 64-bit int")
		}
		val := int64(binary.LittleEndian.Uint64(p[1:9]))
		return nil, val, true, nil
	}

	if encoding == LP_ENCODING_32BIT_STR {
		// 32-bit 字符串
		if len(p) < 5 {
			return nil, 0, false, errors.New("invalid 32-bit string")
		}
		strLen := int(binary.LittleEndian.Uint32(p[1:5]))
		if strLen+5 > len(p) {
			return nil, 0, false, errors.New("invalid 32-bit string")
		}
		val := make([]byte, strLen)
		copy(val, p[5:5+strLen])
		return val, 0, false, nil
	}

	return nil, 0, false, errors.New("unknown encoding")
}

// First 获取第一个元素
func (lp *ListpackFull) First() []byte {
	if lp.getNumElements() == 0 {
		return nil
	}
	return lp.data[LP_HDR_SIZE:]
}

// Next 获取下一个元素
func (lp *ListpackFull) Next(p []byte) ([]byte, error) {
	if len(p) == 0 {
		return nil, errors.New("invalid pointer")
	}

	// 获取当前元素长度
	entryLen, err := lp.getEntryLen(p)
	if err != nil {
		return nil, err
	}

	// 计算当前元素在数据中的位置
	currentPos := len(lp.data) - int(lp.getTotalBytes()) + len(p)

	// 获取 backlen 长度
	backlenStart := currentPos + entryLen
	if backlenStart >= len(lp.data) {
		return nil, errors.New("invalid backlen")
	}

	backlenSize := lp.decodeBacklenSize(lp.data[backlenStart:])

	// 下一个元素
	nextStart := currentPos + entryLen + backlenSize
	if nextStart >= len(lp.data) {
		return nil, nil // EOF
	}

	if lp.data[nextStart] == LP_EOF {
		return nil, nil
	}

	return lp.data[nextStart:], nil
}

// Prev 获取上一个元素
func (lp *ListpackFull) Prev(p []byte) ([]byte, error) {
	if len(p) == 0 {
		return nil, errors.New("invalid pointer")
	}

	// 计算当前元素在数据中的位置
	currentPos := len(lp.data) - int(lp.getTotalBytes()) + len(p)
	if currentPos <= LP_HDR_SIZE {
		return nil, nil // 已经是第一个元素
	}

	// 读取 backlen
	backlenStart := currentPos - 1
	backlen, backlenSize := lp.decodeBacklen(lp.data[backlenStart:])

	// 上一个元素的位置
	prevStart := currentPos - int(backlen) - backlenSize
	if prevStart <= LP_HDR_SIZE {
		return nil, errors.New("invalid previous element")
	}

	return lp.data[prevStart:], nil
}

// 编码相关辅助函数

// encodeStringSize 计算字符串编码后的长度
func (lp *ListpackFull) encodeStringSize(strLen int) int {
	if strLen < 64 {
		return 1 + strLen
	} else if strLen < 4096 {
		return 2 + strLen
	} else {
		return 5 + strLen
	}
}

// encodeString 编码字符串
func (lp *ListpackFull) encodeString(buf []byte, s []byte) {
	len := len(s)
	if len < 64 {
		buf[0] = byte(len) | LP_ENCODING_6BIT_STR
		copy(buf[1:], s)
	} else if len < 4096 {
		buf[0] = byte(len>>8) | LP_ENCODING_12BIT_STR
		buf[1] = byte(len)
		copy(buf[2:], s)
	} else {
		buf[0] = LP_ENCODING_32BIT_STR
		binary.LittleEndian.PutUint32(buf[1:5], uint32(len))
		copy(buf[5:], s)
	}
}

// encodeIntegerSize 计算整数编码后的长度
func (lp *ListpackFull) encodeIntegerSize(v int64) int {
	if v >= 0 && v <= 127 {
		return 1
	} else if v >= -4096 && v <= 4095 {
		return 2
	} else if v >= -32768 && v <= 32767 {
		return 3
	} else if v >= -8388608 && v <= 8388607 {
		return 4
	} else if v >= -2147483648 && v <= 2147483647 {
		return 5
	} else {
		return 9
	}
}

// encodeInteger 编码整数
func (lp *ListpackFull) encodeInteger(buf []byte, v int64) {
	if v >= 0 && v <= 127 {
		buf[0] = byte(v)
	} else if v >= -4096 && v <= 4095 {
		if v < 0 {
			v = (1 << 13) + v
		}
		buf[0] = byte(v>>8) | LP_ENCODING_13BIT_INT
		buf[1] = byte(v)
	} else if v >= -32768 && v <= 32767 {
		buf[0] = LP_ENCODING_16BIT_INT
		binary.LittleEndian.PutUint16(buf[1:3], uint16(v))
	} else if v >= -8388608 && v <= 8388607 {
		if v < 0 {
			v = (1 << 24) + v
		}
		buf[0] = LP_ENCODING_24BIT_INT
		buf[1] = byte(v)
		buf[2] = byte(v >> 8)
		buf[3] = byte(v >> 16)
	} else if v >= -2147483648 && v <= 2147483647 {
		buf[0] = LP_ENCODING_32BIT_INT
		binary.LittleEndian.PutUint32(buf[1:5], uint32(v))
	} else {
		buf[0] = LP_ENCODING_64BIT_INT
		binary.LittleEndian.PutUint64(buf[1:9], uint64(v))
	}
}

// encodeBacklenSize 计算 backlen 编码后的长度
func (lp *ListpackFull) encodeBacklenSize(l uint64) int {
	if l <= 127 {
		return 1
	} else if l < 16383 {
		return 2
	} else if l < 2097151 {
		return 3
	} else if l < 268435455 {
		return 4
	} else {
		return 5
	}
}

// encodeBacklen 编码 backlen
func (lp *ListpackFull) encodeBacklen(buf []byte, l uint64) {
	if l <= 127 {
		buf[0] = byte(l)
	} else if l < 16383 {
		buf[0] = byte(l >> 7)
		buf[1] = byte(l&127) | 128
	} else if l < 2097151 {
		buf[0] = byte(l >> 14)
		buf[1] = byte((l>>7)&127) | 128
		buf[2] = byte(l&127) | 128
	} else if l < 268435455 {
		buf[0] = byte(l >> 21)
		buf[1] = byte((l>>14)&127) | 128
		buf[2] = byte((l>>7)&127) | 128
		buf[3] = byte(l&127) | 128
	} else {
		buf[0] = byte(l >> 28)
		buf[1] = byte((l>>21)&127) | 128
		buf[2] = byte((l>>14)&127) | 128
		buf[3] = byte((l>>7)&127) | 128
		buf[4] = byte(l&127) | 128
	}
}

// decodeBacklen 解码 backlen
func (lp *ListpackFull) decodeBacklen(p []byte) (uint64, int) {
	if len(p) == 0 {
		return 0, 0
	}

	val := uint64(0)
	shift := uint(0)
	size := 0

	for i := len(p) - 1; i >= 0; i-- {
		val |= uint64(p[i]&127) << shift
		size++
		if (p[i] & 128) == 0 {
			break
		}
		shift += 7
		if shift > 28 {
			return 0, 0 // 错误
		}
	}

	return val, size
}

// decodeBacklenSize 解码 backlen 的大小
func (lp *ListpackFull) decodeBacklenSize(p []byte) int {
	if len(p) == 0 {
		return 0
	}

	size := 0
	for i := len(p) - 1; i >= 0; i-- {
		size++
		if (p[i] & 128) == 0 {
			break
		}
	}

	return size
}

// getEntryLen 获取元素长度
func (lp *ListpackFull) getEntryLen(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, errors.New("invalid pointer")
	}

	encoding := p[0]

	if encoding&LP_ENCODING_7BIT_UINT_MASK == 0 {
		return 1, nil
	}

	if encoding&LP_ENCODING_6BIT_STR_MASK == LP_ENCODING_6BIT_STR {
		len := int(encoding & 0x3F)
		return 1 + len, nil
	}

	if encoding&LP_ENCODING_13BIT_INT_MASK == LP_ENCODING_13BIT_INT {
		return 2, nil
	}

	if encoding&LP_ENCODING_12BIT_STR_MASK == LP_ENCODING_12BIT_STR {
		if len(p) < 2 {
			return 0, errors.New("invalid 12-bit string")
		}
		strLen := int((int(encoding)&0xF)<<8 | int(p[1]))
		return 2 + strLen, nil
	}

	if encoding == LP_ENCODING_16BIT_INT {
		return 3, nil
	}

	if encoding == LP_ENCODING_24BIT_INT {
		return 4, nil
	}

	if encoding == LP_ENCODING_32BIT_INT {
		return 5, nil
	}

	if encoding == LP_ENCODING_64BIT_INT {
		return 9, nil
	}

	if encoding == LP_ENCODING_32BIT_STR {
		if len(p) < 5 {
			return 0, errors.New("invalid 32-bit string")
		}
		len := int(binary.LittleEndian.Uint32(p[1:5]))
		return 5 + len, nil
	}

	return 0, errors.New("unknown encoding")
}

// grow 扩容
func (lp *ListpackFull) grow(newSize uint32) {
	newData := make([]byte, newSize*2) // 预分配更多空间
	copy(newData, lp.data)
	lp.data = newData
}
