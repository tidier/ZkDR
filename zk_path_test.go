package zk

import (
	"fmt"
	"testing"
)

func TestZkJoinPath(t *testing.T) {
	s := ZkJoinPath("/test", "im", "imr/")
	fmt.Println("joinPath:", s)
}
