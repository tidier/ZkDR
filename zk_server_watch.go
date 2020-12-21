package zk

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

func (zs *TZkServer) doWatchEvent(e zk.Event) {
	switch e.Type {
	case zk.EventNodeChildrenChanged:
		go zs.loadSinkEvents()
		break
	}
}

func (zs *TZkServer) watchEventSink() {
	for {
		select {
		case <-zs.close:
			return
		default:
		}
		_, _, event, err := zs.conn.ChildrenW(zs.eventSinkPath)
		if err != nil && err == zk.ErrNoNode {
			time.Sleep(time.Second)
		}
		select {
		case <-zs.close:
			return
		case e := <-event:
			zs.doWatchEvent(e)
		}
	}
}

func (zs *TZkServer) loadSinkEvents() {
	items, _, err := zs.conn.Children(zs.eventSinkPath)
	if err != nil {
		zs.logger.Errorf("接收事件出错:%s", err)
		return
	}
	for _, event := range items {
		eventPath := zs.eventSinkPath + "/" + event
		// 读取事件参数
		bytes, _, err := zs.conn.Get(eventPath)
		if err != nil {
			zs.logger.Errorf("读取事件:%s参数出错:%v", event, err)
			continue
		}
		var params interface{} = nil
		if len(bytes) > 0 {
			if err := json.Unmarshal(bytes, &params); err != nil {
				zs.logger.Errorf("反序列化事件:%s参数出错:%v", event, err)
				continue
			}
		}
		select {
		case <-zs.close:
			return
		case zs.sinkBuff <- &Event{
			Name:   event,
			Params: params,
		}:
			continue
		}
	}
}
