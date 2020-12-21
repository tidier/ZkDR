package zk

type ServiceModeEvent int32

const (
	SNT_UNKNOWN ServiceModeEvent = iota
	SNT_ONLINE
	SNT_OFFLIEN
	SNT_CHANGED
)

func (snt ServiceModeEvent) String() string {
	switch snt {
	case SNT_ONLINE:
		return "nodeEventOnline"
	case SNT_OFFLIEN:
		return "nodeEventOffline"
	case SNT_CHANGED:
		return "nodeEventChanged"
	default:
		return "nodeEventUnknown"
	}
}

type serviceEvent struct {
	service *TServiceNode
	event   ServiceModeEvent
	args    []interface{}
}

type ServerNodeEventHandler = func(server *TServiceNode, event ServiceModeEvent, args ...interface{})
type NodeEventHandler = func(eventName string, data interface{})
