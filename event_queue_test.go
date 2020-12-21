package zk

import (
	"fmt"
	"testing"
)

func TestEventQueue(t *testing.T) {
	queue := new(eventQueue)
	go func() {
		for i := 0; i < 10; i++ {
			t.Log("put:", i)
			queue.put(&Event{
				Name:   fmt.Sprintf("%d", i),
				Params: nil,
			}, 1000)
		}
	}()
	go func() {
		var i = 0
		for {
			token := queue.peek()
			if token == nil {
				continue
			}
			i++
			if i > 2000 {
				break
			}
			t.Log("peek:", token.Event.Name)
		}
		fmt.Println("============ peek all end.")
	}()
	select {}
}
