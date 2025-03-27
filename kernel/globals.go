package kernel

import (
	"net"

	"github.com/cnlesscode/firstMQServer/configs"
	"github.com/cnlesscode/serverFinder"
)

// 消息存储管道
var MessageChannels = make(map[string]chan []byte)

// 消费消息缓存管道
var ConsumeMessageChannels = make(map[string]*ConsumeMessagesChannel)

// ServerFinder tcp 通信连接
var serverFinderConn net.Conn

// 注册 FirstMQ 服务到 ServerFinder
func RegisterFirstMQService() {
	serverFinder.Register(
		"register",
		configs.ServerFinderConfig.Host+":"+configs.ServerFinderConfig.Port,
		"firstMQServers",
		configs.CurrentIP+":"+configs.FirstMQConfig.Port,
	)
}
