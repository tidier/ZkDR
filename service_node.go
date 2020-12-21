package zk

import "strconv"

// ServiceNode 服务节点
type TServiceNode struct {
	// 节点id
	NodeId int `json:"node_id"`
	// 发布的地址
	Host string `json:"host"`
	Ver  int32  `json:"ver"`
}

// TServiceDesc 服务描述信息
type TServiceDesc struct {
	// 服务名称
	ServiceName string `json:"service_name"`
	// 服务描述
	ServiceDesc string `json:"service_desc"`
	// 服务版本号
	VerCode int `json:"ver_code"`
}

// EventEmitterPath 服务事件发射器节点路径
func (sd TServiceDesc) eventEmitterPath(root string) string {
	return ZkJoinPath(root, sd.ServiceName, eventEmitterPath)
}

// EventSinkPath 服务提供方事件接收器
func (sd TServiceDesc) eventSinkPath(root string) string {
	return ZkJoinPath(root, sd.ServiceName, eventSinkPath)
}

// instanceContainerPath 服务实例容器存放节点路径
func (sd TServiceDesc) instanceContainerPath(root string) string {
	return ZkJoinPath(root, sd.ServiceName, instancePath)
}

// ServicePath 服务存放节点路径
func (sd TServiceDesc) servicePath(root string) string {
	return ZkJoinPath(root, sd.ServiceName)
}

// instancePath 服务实例节点
func (sn TServiceNode) InstancePath(root string, desc *TServiceDesc) string {
	return ZkJoinPath(root, desc.ServiceName, instancePath, strconv.Itoa(sn.NodeId))
}
