package zk

import (
	"github.com/go-xe2/x/os/xlog"
	"github.com/go-xe2/x/type/t"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

// 监听服务变动
func (sl *TZkListener) watchInstanceLoop() {
	for {
		select {
		case <-sl.close:
			return
		default:
		}
		_, _, event, err := sl.conn.ChildrenW(sl.instancePath)
		if err != nil && err != zk.ErrNoNode {
			return
		} else if err == zk.ErrNoNode {
			// 节点不存在时，等待创建
			time.Sleep(time.Second)
			continue
		}
		select {
		case <-sl.close:
			return
		case e := <-event:
			switch e.Type {
			case zk.EventNodeChildrenChanged:
				go sl.loadInstances()
				break
			case zk.EventNodeDeleted:
				sl.clearInstances()
				break
			}
		}
	}
}

func (sl *TZkListener) loadInstances() {
	paths, _, err := sl.conn.Children(sl.instancePath)
	if err != nil {
		xlog.Error(err)
	}
	sl.svcInstanceLock.Lock()
	defer sl.svcInstanceLock.Unlock()
	sl.serviceInstances = make(map[int]*TServiceNode)
	for _, nodeName := range paths {
		nodeId := t.Int(nodeName)
		instancePath := ZkJoinPath(sl.instancePath, nodeName)
		// 加载子节点数据
		data, stat, err := sl.conn.Get(instancePath)
		if err != nil {
			sl.logger.Errorf("读取服务节点:%s出错:%v", nodeName, err)
			continue
		}
		node := &TServiceNode{
			NodeId: nodeId,
			Host:   string(data),
			Ver:    stat.Version,
		}
		// 放到事件队列
		sl.putServiceNodeEvent(node, SNT_ONLINE)
		// 监听节点变动
		go sl.watchInstanceNodeChanged(nodeId, instancePath)
	}
}

func (sl *TZkListener) putServiceNodeEvent(node *TServiceNode, event ServiceModeEvent, args ...interface{}) {
	select {
	case <-sl.close:
		return
	case sl.serviceEvents <- serviceEvent{
		service: node,
		event:   event,
		args:    args,
	}:
		return
	}
}

func (sl *TZkListener) clearInstances() {
	sl.svcInstanceLock.RLock()
	defer sl.svcInstanceLock.Unlock()
	for _, v := range sl.serviceInstances {
		sl.putServiceNodeEvent(v, SNT_OFFLIEN)
	}
	sl.serviceInstances = make(map[int]*TServiceNode)
}

func (sl *TZkListener) watchInstanceNodeChanged(nodeId int, path string) {
	for {
		_, _, event, err := sl.conn.ExistsW(path)
		if err != nil {
			if err != zk.ErrNoNode {
				sl.logger.Errorf("监听服务节点%d出错:%s", nodeId, err)
			}
			return
		}
		select {
		case <-sl.close:
			return
		case e := <-event:
			switch e.Type {
			case zk.EventNodeDeleted:
				// 删除结点时不需要再次监听
				if svc := sl.getNodeInfo(nodeId); svc != nil {
					sl.putServiceNodeEvent(svc, SNT_OFFLIEN)
				}
				break
			case zk.EventNodeDataChanged:
				bytes, _, err := sl.conn.Get(path)
				if err != nil {
					break
				}
				if svc := sl.getNodeInfo(nodeId); svc != nil {
					sl.putServiceNodeEvent(svc, SNT_CHANGED, bytes)
				} else {
					node := &TServiceNode{
						NodeId: nodeId,
						Host:   string(bytes),
						Ver:    0,
					}
					sl.putServiceNodeEvent(node, SNT_ONLINE)
				}
				break
			}
		}
	}
}

func (sl *TZkListener) getNodeInfo(nodeId int) *TServiceNode {
	sl.svcInstanceLock.RLock()
	defer sl.svcInstanceLock.RUnlock()
	if v, ok := sl.serviceInstances[nodeId]; ok {
		return v
	}
	return nil
}

func (sl *TZkListener) removeServerInfo(nodeId int) {
	sl.svcInstanceLock.Lock()
	defer sl.svcInstanceLock.Unlock()
	if _, ok := sl.serviceInstances[nodeId]; ok {
		delete(sl.serviceInstances, nodeId)
	}
}

func (sl *TZkListener) storeServerInfo(svc *TServiceNode) {
	sl.svcInstanceLock.Lock()
	defer sl.svcInstanceLock.Unlock()
	sl.serviceInstances[svc.NodeId] = svc
}

func (sl *TZkListener) readServiceEventLoop() {
	for {
		select {
		case <-sl.close:
			return
		default:
		}
		env, ok := <-sl.serviceEvents
		if !ok {
			return
		}
		// 当前版本
		curService := sl.getNodeInfo(env.service.NodeId)
		if env.service.Ver < curService.Ver {
			// 当前版本比该队列的版本大，说明当前的为最后上线/下线的服务
			continue
		}
		if env.event == SNT_OFFLIEN {
			sl.removeServerInfo(env.service.NodeId)
		} else if env.event == SNT_ONLINE {
			sl.storeServerInfo(env.service)
		} else if env.event == SNT_CHANGED {
			svc := env.service
			if len(env.args) > 0 && env.args[0] != nil {
				if bytes, ok := env.args[0].([]byte); ok {
					svc.Host = string(bytes)
				}
			}
		}
		if sl.onServerNodeEvent != nil {
			func() {
				defer func() {
					if err := recover(); err != nil {
						xlog.Error(err)
					}
					sl.onServerNodeEvent(env.service, env.event, env.args...)
				}()
			}()
		}
	}
}
