package zk

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
)

// init 初始化服务提供者
func (zs *TZkServer) initServer() error {
	if err := zs.ensureRoot(); err != nil {
		return err
	}
	if err := zs.ensureServerInfo(); err != nil {
		return err
	}
	go zs.emitPeekLoop()
	go zs.readEventLoop()
	go zs.watchEventSink()
	return nil
}

// ensureRoot 创建服务根据节点
func (zs *TZkServer) ensureRoot() error {
	return EnsureRoot(zs.conn, zs.root)
}

// ensureServerInfo 生成服务的在线状态节点，事件通知节点
func (zs *TZkServer) ensureServerInfo() error {
	if err := EnsureServiceNode(zs.conn, zs.root, &zs.TServiceDesc); err != nil {
		return err
	}
	// 节点已经存，直接更新服务地址信息，如果服务节点配置的id相同，会覆盖其他服输的配置
	// 创建服务实例
	instancePath := zs.TServiceNode.InstancePath(zs.root, &zs.TServiceDesc)
	isExists, stat, err := zs.conn.Exists(instancePath)
	if isExists {
		_, err = zs.conn.Set(instancePath, []byte(zs.Host), stat.Version)
		if err != nil && err != zk.ErrNoNode {
			return err
		}
	}
	// 更新失败或不存在，都重新创建
	_, err = zs.conn.Create(instancePath, []byte(zs.Host), int32(NT_EPHEMERAL), PermAllAcls)
	if err != nil {
		if err == zk.ErrNodeExists {
			// 可能其他进程创建了相同的服务
			return fmt.Errorf("服务ID:%s可以已分配给了其他服务节点，请检查", zs.NodeId)
		}
		return err
	}
	return nil
}
