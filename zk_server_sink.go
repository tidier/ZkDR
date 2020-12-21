package zk

func (zs *TZkServer) doEventSink(event *Event) {
	if event == nil {
		return
	}
	if zs.eventHandler != nil {
		go func() {
			defer func() {
				if e := recover(); e != nil {
					zs.logger.Errorf("回调eventHandler出错:%v", e)
				}
			}()
			zs.eventHandler(event.Name, event.Params)
		}()
	}
	zs.eventLock.RLock()
	defer zs.eventLock.RUnlock()
	if m, ok := zs.eventBinders[event.Name]; ok {
		for _, fn := range m {
			func() {
				defer func() {
					if e := recover(); e != nil {
						zs.logger.Errorf("处理事件%s出错:%v", event.Name, e)
					}
					fn(event.Params)
				}()
			}()
		}
	}
}

func (zs *TZkServer) readEventLoop() {
	for {
		select {
		case <-zs.close:
			return
		default:
		}
		event, ok := <-zs.sinkBuff
		if !ok {
			// 已关闭队列
			return
		}
		if event == nil {
			continue
		}
		zs.doEventSink(event)
	}
}
