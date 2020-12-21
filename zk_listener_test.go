package zk

import (
	"fmt"
	"testing"
)

func TestNewZkListener(t *testing.T) {
	listener, err := NewZkListener(&TZkConfig{
		Hosts:          []string{"47.112.131.64:2181"},
		SessionTimeout: 10 * 60,
	}, "/mny/im", "imr", func(bytes []byte) (ZkServerInfo, error) {
		return NewDefaultServerInfoByData(bytes), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	listener.SetServerPointListener(func(server ZkServerInfo, pointEvent ServerPointEventType, args ...interface{}) {
		fmt.Println("server point event:", server, pointEvent, args)
	})
	listener.SetEventListener(func(eventName string, data interface{}) {
		fmt.Println("event:", eventName, ", data:", data)
	})
	select {}
}
