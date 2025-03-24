### FirstMQ
> First Message Queue ( GraceMQ ) 是一款基于 GO 语言的、完全免费的、高效的、易部署的、易使用的分布式消息中间件。

### FirstMQ 使用手册
https://www.lesscode.work/sections/94de498e71d87a0d011295bf5deb0fd2.html

### 快速启动
#### main.go
```
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

	log.Println("当前服务器IP:" + config.CurrentIP)

	// 启动 firstKV 服务器
	if config.CurrentIP == config.MasterIP || config.MasterIP == "" {
		log.Println("当前为Master主机, 启动FirstKV服务")
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
```

#### configs.ini
```
# 运行模式 debug 调试, release 正式
RunMode=debug

# 全局数据文件存储目录名称
GlobalDataDirName=data
# 主服务器IP配置
# 如果主机IP与主服务器IP一致，则此主机位 master
MasterIP=192.168.31.188


# Message Queue 配置
[FirstMQ]
# 话题数据本地存储目录名称
DataDir=topics
# 每个分片最多存储数据数量
NumberOfFragmented=20000
# TCP 服务端口
TCPPort=8801
# WebSocket 服务端口
WebSocketPort=8802
# 生产消息时，话题数据临时存储管道容量，默认10万
ChannelCapactiyForProduct=100000
# 落盘时每次最多读取数据数量
MaxNumberForRepaireToDisk=2000
# 消费服务每次向管道填充消息数量
# 并发高时此数据设置高一些
# 最大数量不能超过一个分片存储容量
FillNumberEachTime=1000
# 落盘时空闲休眠时间，单位毫秒
# 此处延迟时间越短 CPU 占用越高
# 主机运算速度快应该设置大一些，否则循环会占用 CPU
# 经测试 50 ~ 200 毫秒比较合理
IdleSleepTimeForWrite=100
# 填充消费消息时空闲休眠时间
# 此处延迟时间越短 CPU 占用越高
# 主机运算速度快应该设置大一些，否则循环会占用 CPU
# 经测试 50 ~ 200 毫秒比较合理
IdleSleepTimeForRead=200


# firstKV 配置
[FirstKV]
# 服务端口
Port=8803
# 数据文件存储目录名称
DataDir=firstKV
```
