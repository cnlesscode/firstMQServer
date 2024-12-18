package kernel

import (
	"log"
	"net"
	"time"

	"github.com/cnlesscode/firstKV"
	"github.com/cnlesscode/firstMQServer/config"
)

// 消息存储管道
var MessageChannels = make(map[string]chan []byte)

// 消费消息缓存管道
var ConsumeMessageChannels = make(map[string]*ConsumeMessagesChannel)

// FirstKV tcp 通信连接
var firstKVConn net.Conn

// 注册 FirstMQ 服务到 FirstKV
func RegisterFirstMQService() {
	step := 0
	var err error
	for {
		firstKVConn, err = net.DialTimeout(
			"tcp",
			config.MasterIP+":"+config.FirstKVConfig.Port,
			time.Millisecond*500,
		)
		if err != nil {
			time.Sleep(time.Second * 1)
			step++
			if step > 5 {
				panic("连接 firstKV 服务失败")
			}
		} else {
			break
		}
	}
	msg := firstKV.ReceiveMessage{
		Action: "add mqServer",
		Key:    "firstMQServers",
		Data: firstKV.FirstMQAddr{
			Host:     config.CurrentIP,
			Port:     config.FirstMQConfig.TCPPort,
			Addr:     config.CurrentIP + ":" + config.FirstMQConfig.TCPPort,
			JoinTime: time.Now().Format("2006-01-02 15:04:05"),
		},
	}
	_, err = firstKV.Send(firstKVConn, msg)
	if err != nil {
		log.Println("向 firstKV 注册服务失败")
	}
}
