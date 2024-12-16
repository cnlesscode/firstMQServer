package main

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/cnlesscode/firstKV"
	"github.com/cnlesscode/firstMQServer/config"
	"github.com/cnlesscode/firstMQServer/kernel"
	"github.com/cnlesscode/firstMQServer/server"
)

/*
进度 : 测试文本及结构体记录
*/
func main() {
	if config.RunMode == "debug" {
		go func() {
			for {
				time.Sleep(time.Second * 5)
				fmt.Printf("协程数 : %v\n", runtime.NumGoroutine())
				fmt.Printf("cap(kernel.MessageChannels[\"default\"]): %v\n", cap(kernel.MessageChannels["test"]))
				fmt.Printf("len(kernel.MessageChannels[\"default\"]): %v\n", len(kernel.MessageChannels["test"]))
			}
		}()
	}

	log.Println("✔ 当前服务器IP : " + config.CurrentIP)

	// 启动 firstKV 服务器
	if config.CurrentIP == config.MasterIP || config.MasterIP == "" {
		log.Println("✔ FirstKV : 当前为Master主机, 启动FirstKV服务")
		go func() {
			firstKV.StartServer(config.FirstKVConfig.Port, config.FirstKVConfig.DataDir)
		}()
	}

	// 开启 WS 服务
	go func() {
		server.StartWSServer()
	}()

	// 开启 TCP 服务
	server.StartFirstMQTcpServer()
}
