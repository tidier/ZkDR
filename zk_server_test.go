package zk

import (
	"fmt"
	"testing"
	"time"
)

func TestNewZkServer(t *testing.T) {
	zkSvc, err := NewZkServer("/mny/im", NewDefaultServerInfo(ZkServerInfoData{
		Name: "imr",
		Desc: "消息路由服务",
		Ver:  "v1.0.0",
	}, "127.0.0.1:7071"), &TZkConfig{
		Hosts:          []string{"47.112.131.64:2181"},
		SessionTimeout: 10 * 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer zkSvc.Close()
	events := map[int]string{
		0: "event0",
		1: "chat",
		2: "event2",
		3: "modify",
	}
	go func() {
		i := 0
		for {
			eventName := events[i]
			err := zkSvc.Emit(eventName, &struct {
				Value int `json:"value"`
			}{
				Value: i,
			})
			if err != nil {
				fmt.Println("emit error:", err)
			} else {
				fmt.Println("emit event:", eventName, ", data:", i)
			}
			i++
			if i > 3 {
				i = 0
			}
			time.Sleep(2 * time.Second)
		}
	}()
	fmt.Println("zkServer start end.")
	select {
	case <-time.Tick(1 * time.Minute):
	}
}
