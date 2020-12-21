/***
zookeeper 库封装
*/

package zk

import (
	"time"
)

type TZkConfig struct {
	// zookeeper 服务客户端配置
	Hosts []string `json:"hosts"`
	// 会话超时(秒)
	SessionTimeout time.Duration `json:"session_timeout"`
}
