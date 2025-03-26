package kernel

import (
	"log"
	"net"
	"time"

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
	step := 0
	var err error
	for {
		serverFinderConn, err = net.DialTimeout(
			"tcp",
			configs.ServerFinderConfig.Host+":"+configs.ServerFinderConfig.Port,
			time.Millisecond*500,
		)
		if err != nil {
			time.Sleep(time.Second * 1)
			step++
			if step > 5 {
				panic("✘ 连接 ServerFinder 服务失败")
			}
		} else {
			break
		}
	}
	msg := serverFinder.ReceiveMessage{
		Action:  "set",
		MainKey: "firstMQServers",
		ItemKey: configs.CurrentIP + ":" + configs.FirstMQConfig.Port,
		Data:    configs.CurrentIP + ":" + configs.FirstMQConfig.Port,
	}
	_, err = serverFinder.Send(serverFinderConn, msg, true)
	if err != nil {
		log.Println("✘ 向 ServerFinder 注册服务失败")
	}
}
