package zk

import (
	"encoding/json"
	"github.com/go-xe2/x/os/xlog"
	"github.com/go-xe2/x/type/t"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/tidier/logger"
	"sync"
	"time"
)

type TServiceNodeInstance struct {
	NodeId int
	Host   string
}

type TZkListener struct {
	ServiceName string `json:"service_name"`
	ServiceDesc string `json:"service_desc"`
	ServiceVer  int    `json:"service_ver"`
	conn        *zk.Conn
	// 服务根节点
	root string
	// 服务名称
	servicePath       string
	instancePath      string
	eventPath         string
	emitterPath       string
	pointVer          map[int64]int64
	svcInstanceLock   sync.RWMutex
	eventLock         sync.RWMutex
	close             chan byte
	binderLock        sync.RWMutex
	events            chan *Event
	serviceEvents     chan serviceEvent
	onBinders         map[string]func(data interface{})
	onEvent           EventListenerHandler
	config            TZkConfig
	onServerNodeEvent ServerNodeEventHandler
	// 服务实例节点地址信息
	serviceInstances map[int]*TServiceNode
	logger           logger.ILogger
}

func NewZkListener(cfg *TZkConfig, root string, serviceName string) (*TZkListener, error) {
	result := &TZkListener{
		conn:             nil,
		root:             root,
		logger:           logger.Sugar,
		ServiceName:      serviceName,
		ServiceDesc:      "",
		ServiceVer:       -1,
		instancePath:     "",
		eventPath:        "",
		pointVer:         make(map[int64]int64),
		svcInstanceLock:  sync.RWMutex{},
		eventLock:        sync.RWMutex{},
		close:            make(chan byte),
		binderLock:       sync.RWMutex{},
		events:           make(chan *Event, 512),
		serviceEvents:    make(chan serviceEvent, 512),
		onBinders:        make(map[string]func(data interface{})),
		serviceInstances: make(map[int]*TServiceNode),
		config: TZkConfig{
			Hosts:          []string{"127.0.0.1:2181"},
			SessionTimeout: 10 * 60,
		},
	}
	if cfg != nil {
		result.config.Hosts = cfg.Hosts
		result.config.SessionTimeout = cfg.SessionTimeout
	}
	conn, _, err := zk.Connect(result.config.Hosts, result.config.SessionTimeout*time.Second)
	if err != nil {
		return nil, err
	}
	result.conn = conn
	result.servicePath = ZkJoinPath(root, serviceName)
	result.instancePath = ZkJoinPath(root, serviceName, instancePath)
	result.eventPath = ZkJoinPath(root, serviceName, eventEmitterPath)
	result.emitterPath = ZkJoinPath(root, serviceName, eventSinkPath)

	if err := result.initListener(); err != nil {
		return nil, err
	}
	return result, nil
}

// Use 使用插件
func (sl *TZkListener) Use(plugin interface{}) {
	if lg, ok := plugin.(logger.ILogger); ok {
		sl.logger = lg
	}
}

func (sl *TZkListener) initListener() error {
	if err := sl.ensureRoot(); err != nil {
		return err
	}
	if err := sl.ensureService(); err != nil {
		return err
	}
	go sl.peekEventLoop()
	go sl.peekServerPointEventLoop()
	go sl.watchPointLoop()
	go sl.watchEventsLoop()
	return nil
}

func (sl *TZkListener) ensureRoot() error {
	return EnsureRoot(sl.conn, sl.root)
}

func (sl *TZkListener) ensureService() error {

	return EnsureServerDirs(sl.conn, sl.root, sl.serverName, "")
}

func (sl *TZkListener) pointName2PointVer(pointName string) int64 {
	if pointName == "" {
		return -1
	}
	return t.Int64(pointName[1:])
}

// AllPoints 获取服务列表
func (sl *TZkListener) AllPoints() []ZkServerInfo {
	sl.pointLock.RLock()
	defer sl.pointLock.RUnlock()
	results := make([]ZkServerInfo, len(sl.points))
	i := 0
	for _, v := range sl.points {
		results[i] = v
	}
	return results
}

func (sl *TZkListener) loadEvents() {
	events, _, err := sl.conn.Children(sl.eventPath)
	if err != nil {
		xlog.Error(err)
		return
	}
	sl.eventLock.Lock()
	defer sl.eventLock.Unlock()
	removeKeys := make([][]interface{}, 0)
	for _, event := range events {
		path := ZkJoinPath(sl.eventPath, event)
		bytes, stat, err := sl.conn.Get(path)
		if err != nil && err != zk.ErrNodeExists {
			xlog.Error(err)
			continue
		}
		var data interface{}
		if err := json.Unmarshal(bytes, &data); err == nil {
			select {
			case <-sl.close:
				return
			case sl.events <- eventData{
				eventName: event,
				eventData: data,
			}:
				removeKeys = append(removeKeys, []interface{}{path, stat})
				continue
			}
		}
	}
	// 删除事件节点
	for _, val := range removeKeys {
		path := val[0].(string)
		stat := val[1].(*zk.Stat)
		err := sl.conn.Delete(path, stat.Version)
		if err != nil {
			xlog.Error(err)
		}
	}
}

func (sl *TZkListener) peekEventLoop() {
	for {
		select {
		case <-sl.close:
			return
		case envData, ok := <-sl.events:
			if !ok {
				return
			}
			go sl.doEvent(envData.eventName, envData.eventData)
		}
	}
}

// doEvent 触发事件
func (sl *TZkListener) doEvent(eventName string, data interface{}) {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				xlog.Error(e)
			}
			if sl.onEvent != nil {
				sl.onEvent(eventName, data)
			}
		}()
	}()
	sl.binderLock.RLock()
	defer sl.binderLock.RUnlock()
	if v, ok := sl.onBinders[eventName]; ok {
		go func(d interface{}) {
			defer func() {
				if e := recover(); e != nil {
					xlog.Error(e)
				}
				v(d)
			}()
		}(data)
	}
}

//  监听事件变动
func (sl *TZkListener) watchEventsLoop() {
	for {
		select {
		case <-sl.close:
			return
		default:
		}
		_, _, event, err := sl.conn.ChildrenW(sl.eventPath)
		if err != nil && err != zk.ErrNodeExists {
			xlog.Error(err)
			return
		} else if err == zk.ErrNodeExists {
			time.Sleep(time.Second)
			continue
		}
		select {
		case <-sl.close:
			return
		case env := <-event:
			switch env.Type {
			case zk.EventNodeChildrenChanged:
				go sl.loadEvents()
				break
			case zk.EventNodeDeleted:
				return
			}
		}
	}
}

// On 绑定事件
func (sl *TZkListener) On(event string, callback func(data interface{})) {
	sl.binderLock.Lock()
	defer sl.binderLock.Unlock()
	sl.onBinders[event] = callback
}

func (sl *TZkListener) SetEventListener(callback func(eventName string, data interface{})) {
	sl.onEvent = callback
}

func (sl *TZkListener) SetServerPointListener(listener ServerPointEventHandler) {
	sl.onServerPointEvent = listener
}

// Close 关闭监听器
func (sl *TZkListener) Close() {
	select {
	case <-sl.close:
		return
	default:
	}
	close(sl.close)
	if sl.conn != nil {
		sl.conn.Close()
		sl.conn = nil
	}
}

func (sl *TZkListener) getPointVer(pointId int64) int64 {
	sl.pointLock.RLock()
	defer sl.pointLock.RUnlock()
	if v, ok := sl.pointVer[pointId]; ok {
		return v
	} else {
		return -1
	}
}
