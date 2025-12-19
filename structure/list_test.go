package structure

import "testing"

func TestNewList(t *testing.T) {
	// 创建列表
	list := NewList()

	// 添加元素
	list.Push([]byte("hello"), 1) // TAIL
	list.Push([]byte("world"), 1) // TAIL
	list.Push([]byte("redis"), 0) // HEAD

	// 获取长度
	println("List length:", list.Len())

	// 弹出元素
	value, err := list.Pop(0) // HEAD
	if err != nil {
		println("Error:", err.Error())
	} else {
		println("Popped:", string(value))
	}
}
