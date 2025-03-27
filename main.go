package main

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/cnlesscode/firstMQServer/configs"
	"github.com/cnlesscode/firstMQServer/kernel"
	"github.com/cnlesscode/firstMQServer/server"
	"github.com/cnlesscode/serverFinder"
)

func main() {
	if configs.RunMode == "debug" {
		go func() {
			for {
				time.Sleep(time.Second * 5)
				fmt.Printf("协程数 : %v\n", runtime.NumGoroutine())
				fmt.Printf("cap(kernel.MessageChannels[\"test\"]): %v\n", cap(kernel.MessageChannels["test"]))
				fmt.Printf("len(kernel.MessageChannels[\"test\"]): %v\n", len(kernel.MessageChannels["test"]))
			}
		}()
	}

	log.Println("当前服务器IP:" + configs.CurrentIP)

	// 启动 ServerFinder 服务
	// 开启条件 : config.ServerFinderConfig.Enable == "on"
	go func() {
		serverFinder.Start(configs.ServerFinderConfig)
	}()

	// 开启 WS 服务
	go func() {
		server.StartWSServer()
	}()

	// 开启 TCP 服务
	server.StartFirstMQTcpServer()
}
