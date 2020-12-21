package zk

import (
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/tidier/logger"
	"sync"
	"time"
)

// zookeeper 服务端提供者
type TZkServer struct {
	TServiceNode
	TServiceDesc
	// 服务发布地址
	Host string `json:"host"`
	conn *zk.Conn
	// 服务提供者根节点
	root string `json:"root"`
	// 服务配置
	cfg *TZkConfig
	// 事件发送器路径
	eventEmitterPath string
	// 事件接收器路径
	eventSinkPath string
	sinkBuff      chan *Event
	emitBuff      chan *Event
	logger        logger.ILogger
	eventBinders  map[string]map[*EventListenerHandler]EventListenerHandler
	eventLock     sync.RWMutex
	eventHandler  AllEventListenerHandler
	close         chan byte
}

// NewServer 创建服务提供者
func NewServer(root string, cfg *TZkConfig, desc *TServiceDesc, node *TServiceNode, host string) (*TZkServer, error) {
	conn, _, err := zk.Connect(cfg.Hosts, cfg.SessionTimeout*time.Second)
	if err != nil {
		return nil, err
	}
	result := new(TZkServer)
	result.logger = logger.Sugar
	result.conn = conn
	result.root = root
	result.cfg = cfg
	result.Host = host
	result.close = make(chan byte)
	result.emitBuff = make(chan *Event, 512)
	result.sinkBuff = make(chan *Event, 512)
	result.eventBinders = make(map[string]map[*EventListenerHandler]EventListenerHandler)
	if desc != nil {
		result.TServiceDesc = *desc
	}
	if node != nil {
		result.TServiceNode = *node
	}
	result.eventEmitterPath = result.TServiceDesc.eventEmitterPath(root)
	result.eventSinkPath = result.TServiceDesc.eventSinkPath(root)
	if err := result.initServer(); err != nil {
		return nil, err
	}
	return result, nil
}

// Use 使用插件, ILogger, AllEventListenerHandler等
func (zs *TZkServer) Use(plugin interface{}) {
	if l, ok := plugin.(logger.ILogger); ok {
		zs.logger = l
	} else if el, ok := plugin.(AllEventListenerHandler); ok {
		zs.eventHandler = el
	}
}

// On 绑定事件
func (zs *TZkServer) On(event string, handler EventListenerHandler) {
	zs.eventLock.Lock()
	defer zs.eventLock.Unlock()
	if m, ok := zs.eventBinders[event]; ok {
		if _, ok := m[&handler]; ok {
			return
		}
		m[&handler] = handler
	} else {
		m := make(map[*EventListenerHandler]EventListenerHandler)
		m[&handler] = handler
		zs.eventBinders[event] = m
	}
}

// Off 解绑事件, eventId传-1时解析该事件的所有监听器
func (zs *TZkServer) Off(event string, handler *EventListenerHandler) {
	zs.eventLock.Lock()
	defer zs.eventLock.Unlock()
	if handler == nil {
		delete(zs.eventBinders, event)
	} else {
		if m, ok := zs.eventBinders[event]; ok {
			delete(m, handler)
		}
	}
}

// Emit 发送事件给消息者
func (zs *TZkServer) Emit(eventName string, data interface{}) error {
	if !zs.EnableEmit() {
		return errors.New("当前zk连接不可用")
	}
	var event = &Event{
		Name:   eventName,
		Params: data,
	}
	select {
	case <-zs.close:
		return errors.New("已经关闭服务")
	case zs.emitBuff <- event:
	}
	return nil
}

// EnableEmit 检查是否可以发送事件信息
func (zs *TZkServer) EnableEmit() bool {
	select {
	case <-zs.close:
		return false
	default:
	}
	return zs.conn != nil
}

// Close 关闭服务
func (zs *TZkServer) Close() {
	select {
	case <-zs.close:
		return
	default:
	}
	close(zs.close)
	if zs.conn != nil {
		zs.conn.Close()
		zs.conn = nil
	}
}
