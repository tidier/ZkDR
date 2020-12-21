package zk

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

// readServiceData 读取服务节点信息，并监听event, instance
func (sl *TZkListener) readServiceData() {
	bytes, _, err := sl.conn.Get(sl.servicePath)
	if err != nil {
		sl.logger.Errorf("读取服务信息出错:%v", err)
		return
	}
	node, err := decodeServiceNode(bytes)
	if err != nil {
		sl.logger.Errorf("反序列化服务信息出错:%v", err)
		return
	}
	sl.ServiceDesc = node.ServiceDesc
	sl.ServiceVer = node.VerCode
	// 监听event, instance
}

func (sl *TZkListener) resetServiceData() {
	sl.ServiceDesc = ""
	sl.ServiceVer = -1
}

// 监听服务目录是否存在
func (sl *TZkListener) watchServiceExistsPath() {
	// 检查节点是否存在
	exists, _, err := sl.conn.Exists(sl.servicePath)
	if err != nil {
		sl.logger.Errorf("检查服务节点%s出错:%v", sl.ServiceName, err)
		return
	}
	if exists {
		// 节点已经存在, 开始读取服务信息与监听instance, events, point
		sl.readServiceData()
		return
	}
	// 监听服务创建
	for {
		select {
		case <-sl.close:
			return
		default:
		}
		_, _, event, err := sl.conn.ExistsW(sl.servicePath)
		if err != nil {
			sl.logger.Error(err)
			time.Sleep(time.Second)
			continue
		}
		select {
		case <-sl.close:
			return
		case evn := <-event:
			switch evn.Type {
			case zk.EventNodeCreated:
				// 节点创建
				sl.readServiceData()
				return
			case zk.EventNodeDeleted:
				// 节点删除
				sl.resetServiceData()
				return
			}
		}
	}
}
