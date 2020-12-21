package zk

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
)

type NodeType int32

const (
	// NT_PERSISTENT 持久化目录
	NT_PERSISTENT NodeType = 0 // FlagPersistent
	// NT_EPHEMERAL 临时目录，断开时删除
	NT_EPHEMERAL NodeType = 1 // FlagEphemeral
	// NT_PERSISTENT_SEQ 持久化顺序编号目录
	NT_PERSISTENT_SEQ NodeType = 2 // FlagPersistent | FlagSequence
	// NT_EPHEMERAL_SEQ 临时顺序编号目录，断开时删除
	NT_EPHEMERAL_SEQ NodeType = 3 // FlagEphemeral | FlagSequence
)

// 所有权限
var PermAllAcls = zk.WorldACL(zk.PermAll)

const (
	// 服务事件通知节点
	eventEmitterPath = "eventEmitter"
	// 服务实例存放节点
	instancePath = "instances"
	// 服务事件接收节点
	eventSinkPath = "eventSink"
)

// 确保root路径存在
func EnsureRoot(conn *zk.Conn, root string) error {
	exists, _, err := conn.Exists(root)
	if err != nil {
		return err
	}
	if !exists {
		// 创建根节点
		pathToken := MakePathToken(root)
		for _, path := range pathToken {
			_, err := conn.Create(path, []byte(""), int32(NT_PERSISTENT), PermAllAcls)
			if err != nil && err != zk.ErrNodeExists {
				return err
			}
		}
	}
	return nil
}

// addServicePath 添加服务节点
func addServicePath(conn *zk.Conn, path string, data []byte) error {
	_, err := conn.Create(path, data, int32(NT_PERSISTENT), PermAllAcls)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	return nil
}

// updateServicePath 更新服务节点, 如果服务不存在则自动创建
func updateServicePath(conn *zk.Conn, path string, data []byte, ver int32) error {
	_, err := conn.Set(path, data, ver)
	if err != nil {
		if err == zk.ErrNoNode {
			// 节点不存在时创建节点
			return addServicePath(conn, path, data)
		}
		return err
	}
	return err
}

// encodeServiceNode 序列化节点信息
func encodeServiceNode(node *TServiceNode) ([]byte, error) {
	return json.Marshal(node)
}

// decodeServiceNode 反序列化节点信息
func decodeServiceNode(data []byte) (*TServiceNode, error) {
	var node TServiceNode
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

// encodeServiceDesc 序列化服务描述
func encodeServiceDesc(desc *TServiceDesc) ([]byte, error) {
	return json.Marshal(desc)
}

// decodeServiceDesc 反序列化服务描述
func decodeServiceDesc(data []byte) (*TServiceDesc, error) {
	var desc TServiceDesc
	if err := json.Unmarshal(data, &desc); err != nil {
		return nil, err
	}
	return &desc, nil
}

// EnsureServiceNode 确保服务目录结构存在
func EnsureServiceNode(conn *zk.Conn, root string, node *TServiceDesc) error {
	servicePath := node.servicePath(root)
	bytes, stat, err := conn.Get(servicePath)
	if err != nil {
		if err == zk.ErrNoNode {
			// 节点不存在，创建节点
			bytes, err = encodeServiceDesc(node)
			if err != nil {
				return err
			}
			if err := addServicePath(conn, servicePath, bytes); err != nil {
				return err
			}
		}
		return err
	}
	// 检查服务版本，是否需要更新信息
	oldNode, err := decodeServiceDesc(bytes)
	if err != nil {
		// 反序列原信息用出，强制更新
		bytes, err = encodeServiceDesc(node)
		if err != nil {
			return err
		}
		if err := updateServicePath(conn, servicePath, bytes, stat.Version); err != nil {
			return err
		}
	} else {
		if oldNode.VerCode < node.VerCode {
			bytes, err = encodeServiceDesc(node)
			if err != nil {
				return err
			}
			// 更新信息
			if err := updateServicePath(conn, servicePath, bytes, stat.Version); err != nil {
				return err
			}
		}
	}
	// 创建实例接点
	instancePath := node.instanceContainerPath(root)
	_, err = conn.Create(instancePath, []byte(""), int32(NT_PERSISTENT), PermAllAcls)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	// 创建事件发送器节点
	eventEmitterPath := node.eventEmitterPath(root)
	_, err = conn.Create(eventEmitterPath, []byte(""), int32(NT_PERSISTENT), PermAllAcls)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	// 创建事件接收器节点
	eventSinkPath := node.eventSinkPath(root)
	_, err = conn.Create(eventSinkPath, []byte(""), int32(NT_PERSISTENT), PermAllAcls)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	return nil
}
