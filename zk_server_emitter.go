package zk

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
)

func (zs *TZkServer) doEmit(event *Event) {
	if !zs.EnableEmit() {
		return
	}
	var bytes []byte
	if event.Params == nil {
		bytes = []byte("")
	} else {
		var err error
		bytes, err = json.Marshal(event.Params)
		if err != nil {
			zs.logger.Errorf("序列化节点信息出错:%s", err.Error())
			return
		}
	}

	pathName := ZkJoinPath(zs.eventEmitterPath, event.Name)
	exists, stat, err := zs.conn.Exists(pathName)
	if exists {
		// 更新事件数据
		_, err := zs.conn.Set(pathName, bytes, stat.Version)
		if err != nil {
			zs.logger.Errorf("发送事件%s出错%s", event.Name, err.Error())
			return
		}
		return
	}
	// 保存事件数据
	_, err = zs.conn.Create(pathName, bytes, int32(NT_EPHEMERAL), PermAllAcls)
	if err != nil && err != zk.ErrNodeExists {
		// 如果有新的事件已发送，忽略该次事件
		zs.logger.Errorf("发送事件:%s出错:%s", err.Error())
	}
}

func (zs *TZkServer) emitPeekLoop() {
	for {
		select {
		case <-zs.close:
			return
		default:
		}
		event, ok := <-zs.emitBuff
		if !ok {
			return
		}
		if event == nil {
			continue
		}
		zs.doEmit(event)
	}
}
