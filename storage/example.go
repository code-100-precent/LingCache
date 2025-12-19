package storage

import (
	"fmt"
	"time"
)

/*
 * ============================================================================
 * Redis 存储系统使用示例
 * ============================================================================
 *
 * 本文件展示了如何使用 Redis 存储系统进行各种操作。
 */

// ExampleBasicUsage 基本使用示例
func ExampleBasicUsage() {
	// 创建 Redis 服务器（16 个数据库）
	server := NewRedisServer(16)

	// 获取当前数据库（默认是数据库 0）
	db := server.GetCurrentDb()

	// ========== String 操作 ==========
	fmt.Println("=== String 操作 ===")

	// SET key value
	key1 := "name"
	value1 := NewStringObject([]byte("Alice"))
	db.Set(key1, value1)
	fmt.Printf("SET %s %s\n", key1, "Alice")

	// GET key
	obj, err := db.Get(key1)
	if err == nil {
		val, _ := obj.GetStringValue()
		fmt.Printf("GET %s = %s\n", key1, string(val))
	}

	// ========== List 操作 ==========
	fmt.Println("\n=== List 操作 ===")

	// LPUSH list value
	listKey := "mylist"
	listObj := NewListObject()
	db.Set(listKey, listObj)

	list, _ := listObj.GetList()
	list.Push([]byte("world"), 0) // HEAD
	list.Push([]byte("hello"), 0) // HEAD
	fmt.Printf("LPUSH %s hello world\n", listKey)

	// LLEN list
	fmt.Printf("LLEN %s = %d\n", listKey, list.Len())

	// LRANGE list 0 -1
	values, _ := list.Range(0, -1)
	fmt.Printf("LRANGE %s 0 -1 = ", listKey)
	for i, v := range values {
		if i > 0 {
			fmt.Print(" ")
		}
		fmt.Print(string(v))
	}
	fmt.Println()

	// ========== Set 操作 ==========
	fmt.Println("\n=== Set 操作 ===")

	// SADD set member
	setKey := "myset"
	setObj := NewSetObject()
	db.Set(setKey, setObj)

	set, _ := setObj.GetSet()
	set.Add([]byte("apple"))
	set.Add([]byte("banana"))
	set.Add([]byte("apple")) // 重复元素
	fmt.Printf("SADD %s apple banana apple\n", setKey)

	// SCARD set
	fmt.Printf("SCARD %s = %d\n", setKey, set.Card())

	// SISMEMBER set member
	fmt.Printf("SISMEMBER %s apple = %v\n", setKey, set.IsMember([]byte("apple")))

	// ========== ZSet 操作 ==========
	fmt.Println("\n=== ZSet 操作 ===")

	// ZADD zset score member
	zsetKey := "myzset"
	zsetObj := NewZSetObject()
	db.Set(zsetKey, zsetObj)

	zset, _ := zsetObj.GetZSet()
	zset.Add([]byte("alice"), 100.0)
	zset.Add([]byte("bob"), 90.0)
	zset.Add([]byte("charlie"), 95.0)
	fmt.Printf("ZADD %s 100 alice 90 bob 95 charlie\n", zsetKey)

	// ZCARD zset
	fmt.Printf("ZCARD %s = %d\n", zsetKey, zset.Card())

	// ZSCORE zset member
	score, _ := zset.Score([]byte("alice"))
	fmt.Printf("ZSCORE %s alice = %.1f\n", zsetKey, score)

	// ZRANGE zset 0 -1
	entries, _ := zset.Range(0, -1, false)
	fmt.Printf("ZRANGE %s 0 -1 = ", zsetKey)
	for i, entry := range entries {
		if i > 0 {
			fmt.Print(" ")
		}
		fmt.Printf("%s(%.1f)", string(entry.Member()), entry.Score())
	}
	fmt.Println()

	// ========== Hash 操作 ==========
	fmt.Println("\n=== Hash 操作 ===")

	// HSET hash field value
	hashKey := "user:1"
	hashObj := NewHashObject()
	db.Set(hashKey, hashObj)

	hash, _ := hashObj.GetHash()
	hash.Set([]byte("name"), []byte("Alice"))
	hash.Set([]byte("age"), []byte("30"))
	hash.Set([]byte("city"), []byte("Beijing"))
	fmt.Printf("HSET %s name Alice age 30 city Beijing\n", hashKey)

	// HGET hash field
	age, _ := hash.Get([]byte("age"))
	fmt.Printf("HGET %s age = %s\n", hashKey, string(age))

	// HLEN hash
	fmt.Printf("HLEN %s = %d\n", hashKey, hash.Len())

	// HGETALL hash
	allFields := hash.GetAll()
	fmt.Printf("HGETALL %s = ", hashKey)
	for i, entry := range allFields {
		if i > 0 {
			fmt.Print(" ")
		}
		fmt.Printf("%s:%s", string(entry.Field()), string(entry.Value()))
	}
	fmt.Println()

	// ========== 过期时间操作 ==========
	fmt.Println("\n=== 过期时间操作 ===")

	// SET key value EX seconds
	expireKey := "temp_key"
	tempObj := NewStringObject([]byte("temporary"))
	db.Set(expireKey, tempObj)
	db.Expire(expireKey, 5) // 5 秒后过期
	fmt.Printf("SET %s temporary EX 5\n", expireKey)

	// TTL key
	ttl, _ := db.TTL(expireKey)
	fmt.Printf("TTL %s = %d\n", expireKey, ttl)

	// 等待 6 秒后检查
	time.Sleep(6 * time.Second)
	exists := db.Exists(expireKey)
	fmt.Printf("EXISTS %s (after 6s) = %v\n", expireKey, exists)

	// ========== 数据库操作 ==========
	fmt.Println("\n=== 数据库操作 ===")

	// DBSIZE
	fmt.Printf("DBSIZE = %d\n", db.DBSize())

	// KEYS *
	keys := db.Keys("*")
	fmt.Printf("KEYS * = %v\n", keys)

	// SELECT 1
	server.SelectDb(1)
	db1 := server.GetCurrentDb()
	fmt.Println("SELECT 1")

	// 在数据库 1 中设置键
	db1.Set("db1_key", NewStringObject([]byte("value_in_db1")))
	fmt.Printf("DBSIZE (db 1) = %d\n", db1.DBSize())

	// 切换回数据库 0
	server.SelectDb(0)
	db0 := server.GetCurrentDb()
	fmt.Printf("DBSIZE (db 0) = %d\n", db0.DBSize())
}

// ExampleAdvancedUsage 高级使用示例
func ExampleAdvancedUsage() {
	server := NewRedisServer(16)
	db := server.GetCurrentDb()

	fmt.Println("=== 高级操作示例 ===")

	// 集合运算
	set1 := NewSetObject()
	set1Obj, _ := set1.GetSet()
	set1Obj.Add([]byte("1"))
	set1Obj.Add([]byte("2"))
	set1Obj.Add([]byte("3"))
	db.Set("set1", set1)

	set2 := NewSetObject()
	set2Obj, _ := set2.GetSet()
	set2Obj.Add([]byte("2"))
	set2Obj.Add([]byte("3"))
	set2Obj.Add([]byte("4"))
	db.Set("set2", set2)

	// SINTER set1 set2
	set1Retrieved, _ := db.Get("set1")
	set2Retrieved, _ := db.Get("set2")
	s1, _ := set1Retrieved.GetSet()
	s2, _ := set2Retrieved.GetSet()

	intersection := s1.Inter(s2)
	fmt.Printf("SINTER set1 set2 = %d elements\n", intersection.Card())

	// 有序集合范围查询
	zset := NewZSetObject()
	z, _ := zset.GetZSet()
	z.Add([]byte("player1"), 1000.0)
	z.Add([]byte("player2"), 2000.0)
	z.Add([]byte("player3"), 1500.0)
	z.Add([]byte("player4"), 3000.0)
	db.Set("leaderboard", zset)

	// ZRANGE leaderboard 0 2 (获取前 3 名)
	zRetrieved, _ := db.Get("leaderboard")
	zSet, _ := zRetrieved.GetZSet()
	top3, _ := zSet.Range(0, 2, false)
	fmt.Println("ZRANGE leaderboard 0 2 (top 3):")
	for i, entry := range top3 {
		fmt.Printf("  %d. %s (%.0f points)\n", i+1, string(entry.Member()), entry.Score())
	}

	// Hash 批量操作
	hash := NewHashObject()
	h, _ := hash.GetHash()
	fields := [][]byte{[]byte("name"), []byte("age"), []byte("email")}
	values := [][]byte{[]byte("Bob"), []byte("25"), []byte("bob@example.com")}
	h.MSet(fields, values)
	db.Set("user:2", hash)

	// MGET
	hRetrieved, _ := db.Get("user:2")
	hSet, _ := hRetrieved.GetHash()
	results := hSet.MGet(fields)
	fmt.Println("HGETALL user:2:")
	for i, field := range fields {
		fmt.Printf("  %s: %s\n", string(field), string(results[i]))
	}
}

// RunExamples 运行所有示例
func RunExamples() {
	fmt.Println("========================================")
	fmt.Println("Redis 存储系统使用示例")
	fmt.Println("========================================\n")

	ExampleBasicUsage()

	fmt.Println("\n")

	ExampleAdvancedUsage()
}
