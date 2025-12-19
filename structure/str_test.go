package structure

import (
	"fmt"
	"testing"
)

func TestNewSDS(t *testing.T) {
	sds := NewSDS("hello world")
	fmt.Println(sds)
	u := sdsLen(sds)
	fmt.Println(u)
	fmt.Println(sdsType(sds))
	fmt.Println(sdsHdr8(sds))
}
