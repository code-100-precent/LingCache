package storage

import (
	"bytes"
	"github.com/code-100-precent/LingCache/structure"
)

/*
 * ============================================================================
 * Redis 对象系统 (robj)
 * ============================================================================
 *
 * Redis 使用统一的对象系统来表示所有数据类型。
 * 每个对象包含：
 * - type: 对象类型（STRING、LIST、SET、ZSET、HASH）
 * - encoding: 编码方式（决定底层数据结构）
 * - ptr: 指向实际数据的指针
 * - refcount: 引用计数（用于内存管理）
 *
 * 【对象类型】
 * - OBJ_STRING: 字符串对象
 * - OBJ_LIST: 列表对象
 * - OBJ_SET: 集合对象
 * - OBJ_ZSET: 有序集合对象
 * - OBJ_HASH: 哈希对象
 *
 * 【编码方式】
 * 每种对象类型可能有多种编码方式，根据数据特征自动选择：
 * - String: RAW、INT、EMBSTR
 * - List: LISTPACK、QUICKLIST
 * - Set: INTSET、HT
 * - ZSet: LISTPACK、SKIPLIST
 * - Hash: LISTPACK、HT
 */

// ObjectType 对象类型
type ObjectType byte

const (
	OBJ_STRING ObjectType = 0 // 字符串对象
	OBJ_LIST   ObjectType = 1 // 列表对象
	OBJ_SET    ObjectType = 2 // 集合对象
	OBJ_ZSET   ObjectType = 3 // 有序集合对象
	OBJ_HASH   ObjectType = 4 // 哈希对象
)

// RedisObject Redis 对象
type RedisObject struct {
	Type     ObjectType         // 对象类型
	Encoding structure.Encoding // 编码方式
	Ptr      interface{}        // 指向实际数据的指针
	RefCount int                // 引用计数
}

// NewStringObject 创建字符串对象
func NewStringObject(value []byte) *RedisObject {
	sds := structure.NewSDSFromBytes(value)
	return &RedisObject{
		Type:     OBJ_STRING,
		Encoding: structure.OBJ_ENCODING_RAW,
		Ptr:      sds,
		RefCount: 1,
	}
}

// NewListObject 创建列表对象
func NewListObject() *RedisObject {
	return &RedisObject{
		Type:     OBJ_LIST,
		Encoding: structure.OBJ_ENCODING_LISTPACK,
		Ptr:      structure.NewList(),
		RefCount: 1,
	}
}

// NewSetObject 创建集合对象
func NewSetObject() *RedisObject {
	return &RedisObject{
		Type:     OBJ_SET,
		Encoding: structure.OBJ_ENCODING_INTSET,
		Ptr:      structure.NewSet(),
		RefCount: 1,
	}
}

// NewZSetObject 创建有序集合对象
func NewZSetObject() *RedisObject {
	return &RedisObject{
		Type:     OBJ_ZSET,
		Encoding: structure.OBJ_ENCODING_LISTPACK,
		Ptr:      structure.NewZSet(),
		RefCount: 1,
	}
}

// NewHashObject 创建哈希对象
func NewHashObject() *RedisObject {
	return &RedisObject{
		Type:     OBJ_HASH,
		Encoding: structure.OBJ_ENCODING_LISTPACK,
		Ptr:      structure.NewHash(),
		RefCount: 1,
	}
}

// IncrRefCount 增加引用计数
func (obj *RedisObject) IncrRefCount() {
	obj.RefCount++
}

// DecrRefCount 减少引用计数
func (obj *RedisObject) DecrRefCount() {
	obj.RefCount--
	if obj.RefCount <= 0 {
		// 引用计数为 0，可以释放对象
		// Go 的 GC 会自动处理，这里只是标记
	}
}

// GetStringValue 获取字符串值
func (obj *RedisObject) GetStringValue() ([]byte, error) {
	if obj.Type != OBJ_STRING {
		return nil, ErrWrongType
	}

	sds := obj.Ptr.(structure.SDS)
	return structure.SdsBytes(sds), nil
}

// GetList 获取列表对象
func (obj *RedisObject) GetList() (*structure.RedisList, error) {
	if obj.Type != OBJ_LIST {
		return nil, ErrWrongType
	}
	return obj.Ptr.(*structure.RedisList), nil
}

// GetSet 获取集合对象
func (obj *RedisObject) GetSet() (*structure.RedisSet, error) {
	if obj.Type != OBJ_SET {
		return nil, ErrWrongType
	}
	return obj.Ptr.(*structure.RedisSet), nil
}

// GetZSet 获取有序集合对象
func (obj *RedisObject) GetZSet() (*structure.RedisZSet, error) {
	if obj.Type != OBJ_ZSET {
		return nil, ErrWrongType
	}
	return obj.Ptr.(*structure.RedisZSet), nil
}

// GetHash 获取哈希对象
func (obj *RedisObject) GetHash() (*structure.RedisHash, error) {
	if obj.Type != OBJ_HASH {
		return nil, ErrWrongType
	}
	return obj.Ptr.(*structure.RedisHash), nil
}

// TypeString 返回对象类型的字符串表示
func (obj *RedisObject) TypeString() string {
	switch obj.Type {
	case OBJ_STRING:
		return "string"
	case OBJ_LIST:
		return "list"
	case OBJ_SET:
		return "set"
	case OBJ_ZSET:
		return "zset"
	case OBJ_HASH:
		return "hash"
	default:
		return "unknown"
	}
}

// EncodingString 返回编码方式的字符串表示
func (obj *RedisObject) EncodingString() string {
	switch obj.Encoding {
	case structure.OBJ_ENCODING_RAW:
		return "raw"
	case structure.OBJ_ENCODING_INT:
		return "int"
	case structure.OBJ_ENCODING_HT:
		return "hashtable"
	case structure.OBJ_ENCODING_INTSET:
		return "intset"
	case structure.OBJ_ENCODING_SKIPLIST:
		return "skiplist"
	case structure.OBJ_ENCODING_QUICKLIST:
		return "quicklist"
	case structure.OBJ_ENCODING_LISTPACK:
		return "listpack"
	default:
		return "unknown"
	}
}

// Equal 比较两个对象是否相等（简化实现：比较类型和值）
func (obj *RedisObject) Equal(other *RedisObject) bool {
	if obj.Type != other.Type {
		return false
	}

	switch obj.Type {
	case OBJ_STRING:
		val1, _ := obj.GetStringValue()
		val2, _ := other.GetStringValue()
		return bytes.Equal(val1, val2)
	case OBJ_LIST:
		list1, _ := obj.GetList()
		list2, _ := other.GetList()
		return list1.Len() == list2.Len() // 简化比较
	case OBJ_SET:
		set1, _ := obj.GetSet()
		set2, _ := other.GetSet()
		return set1.Card() == set2.Card() // 简化比较
	case OBJ_ZSET:
		zset1, _ := obj.GetZSet()
		zset2, _ := other.GetZSet()
		return zset1.Card() == zset2.Card() // 简化比较
	case OBJ_HASH:
		hash1, _ := obj.GetHash()
		hash2, _ := other.GetHash()
		return hash1.Len() == hash2.Len() // 简化比较
	default:
		return false
	}
}
