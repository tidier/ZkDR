package zk

import "time"

type QueueToken struct {
	// 事件
	Event *Event `json:"event"`
	// 事件过期时间
	Expire time.Time
	Next   *QueueToken
}

// 事件队列, FIFO
type eventQueue struct {
	l *QueueToken
	r *QueueToken
}

func (es *eventQueue) put(event *Event, expire time.Duration) {
	if es.l == nil && es.r == nil {
		es.l = &QueueToken{
			Event:  event,
			Expire: time.Now().Add(expire),
			Next:   nil,
		}
		es.r = es.l
		return
	} else if es.r != nil && es.l == nil {
		es.l = es.r
	}
	last := &QueueToken{
		Event:  event,
		Expire: time.Now().Add(expire),
		Next:   nil,
	}
	es.r.Next = last
	es.r = last
}

func (es *eventQueue) peek() *QueueToken {
	if es.l == nil && es.r == nil {
		return nil
	} else if es.l == nil && es.r != nil {
		es.l = es.r
	}
	first := es.l
	es.l = first.Next
	if first == es.r {
		es.r = nil
	}
	return first
}
