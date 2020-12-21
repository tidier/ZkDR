package zk

type Event struct {
	// 事件名称
	Name string `json:"name"`
	// 事件参数
	Params interface{} `json:"params"`
}

// 所有事件监听器
type AllEventListenerHandler = func(event string, params interface{})

// EventListenerHandler 单一事件监听器
type EventListenerHandler = func(params interface{})
