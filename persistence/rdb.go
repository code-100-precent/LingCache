package persistence

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/code-100-precent/LingCache/storage"
)

/*
 * ============================================================================
 * RDB 持久化实现
 * ============================================================================
 *
 * RDB 是 Redis 的快照持久化方式，将内存中的数据保存到磁盘。
 *
 * 【RDB 文件格式】
 * +--------+--------+--------+--------+--------+--------+--------+
 * | REDIS  | version|  DB 0  |  DB 1  |  ...   |  DB N  |  EOF   |
 * | (5B)   | (4B)   |        |        |        |        | (1B)   |
 * +--------+--------+--------+--------+--------+--------+--------+
 *
 * 【数据库格式】
 * +--------+--------+--------+--------+--------+
 * | SELECT |  DB #  |  Key1  |  Key2  |  ...   |
 * | (1B)   | (1B)   |        |        |        |
 * +--------+--------+--------+--------+--------+
 *
 * 【键值对格式】
 * +--------+--------+--------+--------+--------+
 * |  Type  |  Key   |  Value |  Expire|  ...   |
 * | (1B)   |        |        | (opt)  |        |
 * +--------+--------+--------+--------+--------+
 */

const (
	RDB_MAGIC                = "REDIS"
	RDB_VERSION              = "0009" // Redis 7.0
	RDB_OPCODE_EOF           = 0xFF
	RDB_OPCODE_SELECTDB      = 0xFE
	RDB_OPCODE_EXPIRETIME_MS = 0xFC
	RDB_OPCODE_EXPIRETIME    = 0xFD
)

// RDBEncoder RDB 编码器
type RDBEncoder struct {
	writer io.Writer
}

// NewRDBEncoder 创建 RDB 编码器
func NewRDBEncoder(writer io.Writer) *RDBEncoder {
	return &RDBEncoder{writer: writer}
}

// Save 保存数据库到 RDB 文件
func (enc *RDBEncoder) Save(server *storage.RedisServer, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	enc.writer = file

	// 写入魔数和版本
	enc.writeString(RDB_MAGIC)
	enc.writeString(RDB_VERSION)

	// 保存每个数据库
	for i := 0; i < server.GetDbNum(); i++ {
		db, err := server.GetDb(i)
		if err != nil {
			continue
		}

		if db.DBSize() == 0 {
			continue
		}

		// 写入 SELECTDB
		enc.writeByte(RDB_OPCODE_SELECTDB)
		enc.writeLength(uint32(i))

		// 保存数据库中的所有键值对
		keys := db.Keys("*")
		for _, key := range keys {
			obj, err := db.Get(key)
			if err != nil {
				continue
			}

			// 检查过期时间
			ttl, _ := db.TTL(key)
			if ttl > 0 {
				// 写入过期时间（毫秒）
				enc.writeByte(RDB_OPCODE_EXPIRETIME_MS)
				expireTime := time.Now().UnixMilli() + ttl*1000
				enc.writeUint64(uint64(expireTime))
			}

			// 写入键值对
			if err := enc.writeKeyValue(key, obj); err != nil {
				return err
			}
		}
	}

	// 写入 EOF
	enc.writeByte(RDB_OPCODE_EOF)

	// 写入校验和（简化实现：跳过）

	return nil
}

// writeKeyValue 写入键值对
func (enc *RDBEncoder) writeKeyValue(key string, obj *storage.RedisObject) error {
	// 写入类型
	enc.writeByte(byte(obj.Type))

	// 写入键
	enc.writeString(key)

	// 写入值
	switch obj.Type {
	case storage.OBJ_STRING:
		return enc.writeStringValue(obj)
	case storage.OBJ_LIST:
		return enc.writeListValue(obj)
	case storage.OBJ_SET:
		return enc.writeSetValue(obj)
	case storage.OBJ_ZSET:
		return enc.writeZSetValue(obj)
	case storage.OBJ_HASH:
		return enc.writeHashValue(obj)
	default:
		return fmt.Errorf("unknown object type: %d", obj.Type)
	}
}

// writeStringValue 写入字符串值
func (enc *RDBEncoder) writeStringValue(obj *storage.RedisObject) error {
	val, err := obj.GetStringValue()
	if err != nil {
		return err
	}
	enc.writeString(string(val))
	return nil
}

// writeListValue 写入列表值
func (enc *RDBEncoder) writeListValue(obj *storage.RedisObject) error {
	list, err := obj.GetList()
	if err != nil {
		return err
	}

	// 写入长度
	enc.writeLength(uint32(list.Len()))

	// 写入所有元素
	values, _ := list.Range(0, -1)
	for _, val := range values {
		enc.writeString(string(val))
	}

	return nil
}

// writeSetValue 写入集合值
func (enc *RDBEncoder) writeSetValue(obj *storage.RedisObject) error {
	set, err := obj.GetSet()
	if err != nil {
		return err
	}

	// 写入长度
	enc.writeLength(uint32(set.Card()))

	// 写入所有成员
	members := set.Members()
	for _, member := range members {
		enc.writeString(string(member))
	}

	return nil
}

// writeZSetValue 写入有序集合值
func (enc *RDBEncoder) writeZSetValue(obj *storage.RedisObject) error {
	zset, err := obj.GetZSet()
	if err != nil {
		return err
	}

	// 写入长度
	enc.writeLength(uint32(zset.Card()))

	// 写入所有元素（member 和 score）
	entries, _ := zset.Range(0, -1, false)
	for _, entry := range entries {
		enc.writeString(string(entry.Member()))
		enc.writeFloat64(entry.Score())
	}

	return nil
}

// writeHashValue 写入哈希值
func (enc *RDBEncoder) writeHashValue(obj *storage.RedisObject) error {
	hash, err := obj.GetHash()
	if err != nil {
		return err
	}

	// 写入长度
	enc.writeLength(uint32(hash.Len()))

	// 写入所有字段值对
	entries := hash.GetAll()
	for _, entry := range entries {
		enc.writeString(string(entry.Field()))
		enc.writeString(string(entry.Value()))
	}

	return nil
}

// 辅助函数

func (enc *RDBEncoder) writeByte(b byte) error {
	_, err := enc.writer.Write([]byte{b})
	return err
}

func (enc *RDBEncoder) writeString(s string) error {
	enc.writeLength(uint32(len(s)))
	_, err := enc.writer.Write([]byte(s))
	return err
}

func (enc *RDBEncoder) writeLength(len uint32) error {
	if len < 254 {
		return enc.writeByte(byte(len))
	} else if len <= 0xFFFF {
		enc.writeByte(254)
		binary.Write(enc.writer, binary.LittleEndian, uint16(len))
		return nil
	} else {
		enc.writeByte(255)
		binary.Write(enc.writer, binary.LittleEndian, len)
		return nil
	}
}

func (enc *RDBEncoder) writeUint64(v uint64) error {
	return binary.Write(enc.writer, binary.LittleEndian, v)
}

func (enc *RDBEncoder) writeFloat64(v float64) error {
	return binary.Write(enc.writer, binary.LittleEndian, v)
}

// RDBDecoder RDB 解码器
type RDBDecoder struct {
	reader io.Reader
}

// NewRDBDecoder 创建 RDB 解码器
func NewRDBDecoder(reader io.Reader) *RDBDecoder {
	return &RDBDecoder{reader: reader}
}

// Load 从 RDB 文件加载数据
func (dec *RDBDecoder) Load(server *storage.RedisServer, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dec.reader = file

	// 读取魔数和版本
	magic := make([]byte, 5)
	if _, err := dec.reader.Read(magic); err != nil {
		return err
	}
	if string(magic[:5]) != RDB_MAGIC {
		return fmt.Errorf("invalid RDB file")
	}

	version := make([]byte, 4)
	if _, err := dec.reader.Read(version); err != nil {
		return err
	}

	// 读取数据库
	currentDB := 0
	for {
		b, err := dec.readByte()
		if err != nil {
			break
		}

		if b == RDB_OPCODE_EOF {
			break
		}

		if b == RDB_OPCODE_SELECTDB {
			dbNum, err := dec.readLength()
			if err != nil {
				return err
			}
			currentDB = int(dbNum)
			continue
		}

		if b == RDB_OPCODE_EXPIRETIME_MS {
			expireTime, err := dec.readUint64()
			if err != nil {
				return err
			}
			// 处理过期时间（简化实现：跳过）
			_ = expireTime
			continue
		}

		// 读取键值对
		objType := storage.ObjectType(b)
		key, err := dec.readString()
		if err != nil {
			return err
		}

		obj, err := dec.readValue(objType)
		if err != nil {
			return err
		}

		// 保存到数据库
		db, err := server.GetDb(currentDB)
		if err != nil {
			continue
		}
		db.Set(key, obj)
	}

	return nil
}

func (dec *RDBDecoder) readByte() (byte, error) {
	b := make([]byte, 1)
	_, err := dec.reader.Read(b)
	return b[0], err
}

func (dec *RDBDecoder) readLength() (uint32, error) {
	b, err := dec.readByte()
	if err != nil {
		return 0, err
	}

	if b < 254 {
		return uint32(b), nil
	} else if b == 254 {
		var len uint16
		err := binary.Read(dec.reader, binary.LittleEndian, &len)
		return uint32(len), err
	} else {
		var len uint32
		err := binary.Read(dec.reader, binary.LittleEndian, &len)
		return len, err
	}
}

func (dec *RDBDecoder) readString() (string, error) {
	len, err := dec.readLength()
	if err != nil {
		return "", err
	}

	data := make([]byte, len)
	_, err = dec.reader.Read(data)
	return string(data), err
}

func (dec *RDBDecoder) readUint64() (uint64, error) {
	var v uint64
	err := binary.Read(dec.reader, binary.LittleEndian, &v)
	return v, err
}

func (dec *RDBDecoder) readValue(objType storage.ObjectType) (*storage.RedisObject, error) {
	switch objType {
	case storage.OBJ_STRING:
		val, err := dec.readString()
		if err != nil {
			return nil, err
		}
		return storage.NewStringObject([]byte(val)), nil

	case storage.OBJ_LIST:
		len, err := dec.readLength()
		if err != nil {
			return nil, err
		}
		listObj := storage.NewListObject()
		list, _ := listObj.GetList()
		for i := uint32(0); i < len; i++ {
			val, err := dec.readString()
			if err != nil {
				return nil, err
			}
			list.Push([]byte(val), 1) // TAIL
		}
		return listObj, nil

	case storage.OBJ_SET:
		len, err := dec.readLength()
		if err != nil {
			return nil, err
		}
		setObj := storage.NewSetObject()
		set, _ := setObj.GetSet()
		for i := uint32(0); i < len; i++ {
			member, err := dec.readString()
			if err != nil {
				return nil, err
			}
			set.Add([]byte(member))
		}
		return setObj, nil

	case storage.OBJ_ZSET:
		len, err := dec.readLength()
		if err != nil {
			return nil, err
		}
		zsetObj := storage.NewZSetObject()
		zset, _ := zsetObj.GetZSet()
		for i := uint32(0); i < len; i++ {
			member, err := dec.readString()
			if err != nil {
				return nil, err
			}
			var score float64
			err = binary.Read(dec.reader, binary.LittleEndian, &score)
			if err != nil {
				return nil, err
			}
			zset.Add([]byte(member), score)
		}
		return zsetObj, nil

	case storage.OBJ_HASH:
		len, err := dec.readLength()
		if err != nil {
			return nil, err
		}
		hashObj := storage.NewHashObject()
		hash, _ := hashObj.GetHash()
		for i := uint32(0); i < len; i++ {
			field, err := dec.readString()
			if err != nil {
				return nil, err
			}
			value, err := dec.readString()
			if err != nil {
				return nil, err
			}
			hash.Set([]byte(field), []byte(value))
		}
		return hashObj, nil

	default:
		return nil, fmt.Errorf("unknown object type: %d", objType)
	}
}
