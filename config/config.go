package config

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/cnlesscode/firstKV"
	"github.com/cnlesscode/gotool"
	"github.com/cnlesscode/gotool/gfs"
	"github.com/cnlesscode/gotool/iniReader"
)

// 运行模式
var RunMode string = "debug"

// 全局数据存储位置
var GlobalDataDir string

// 当前服务器内网IP
var CurrentIP string = gotool.GetLocalIP()

// FirstKV配置
var FirstKVConfig = firstKV.Config{
	Enable: "off",
}

// FirstMQ服务配置
type FirstMQConfigStruct struct {
	// 数据存储目录名称
	DataDir string
	// 每个分片存储数据条目数
	NumberOfFragmented int64
	// 服务监听端口
	TCPPort string
	// HTTP服务端口
	WebSocketPort string
	// 生产消息临时管道缓存长度
	ChannelCapactiyForProduct int
	// 落盘时每次最多读取数据数量
	MaxNumberForRepaireToDisk int64
	// 消费环节每次向管道填充消息数量
	FillNumberEachTime int64
	// 落盘时空闲休眠时间
	IdleSleepTimeForWrite time.Duration
	// 填充消费消息时空闲休眠时间
	IdleSleepTimeForRead time.Duration
}

var FirstMQConfig = &FirstMQConfigStruct{}

func init() {
	configFile := gotool.Root + "configs.ini"
	if !gfs.FileExists(configFile) {
		panic("配置文件不存在")
	}
	iniReader := iniReader.New(configFile)

	// 1. 运行模式
	RunMode = iniReader.String("", "RunMode")

	// 2. 全局数据目录，以系统分隔符结尾
	GlobalDataDir = path.Join(gotool.Root, iniReader.String("", "GlobalDataDirName"))
	fmt.Printf("GlobalDataDir: %v\n", GlobalDataDir)
	// 2.1 检查全局数据目录
	if !gfs.DirExists(GlobalDataDir) {
		err := os.Mkdir(GlobalDataDir, 0777)
		if err != nil {
			panic("数据目录创建失败: " + err.Error() + "\n")
		}
	}

	// 3. FirstKV 服务地址
	FirstKVConfig.Host = iniReader.String("FirstKV", "Host")
	FirstKVConfig.Port = iniReader.String("FirstKV", "Port")
	FirstKVConfig.DataLogsDir = path.Join(GlobalDataDir, iniReader.String("FirstKV", "DataLogsDir"))
	FirstKVConfig.Enable = iniReader.String("FirstKV", "Enable")

	// 4. FirstMQ 服务配置
	FirstMQConfig.DataDir = path.Join(GlobalDataDir, iniReader.String("FirstMQ", "DataDir"))
	FirstMQConfig.NumberOfFragmented = iniReader.Int64("FirstMQ", "NumberOfFragmented")
	FirstMQConfig.TCPPort = iniReader.String("FirstMQ", "TCPPort")
	FirstMQConfig.WebSocketPort = iniReader.String("FirstMQ", "WebSocketPort")
	FirstMQConfig.ChannelCapactiyForProduct = iniReader.Int("FirstMQ", "ChannelCapactiyForProduct")
	FirstMQConfig.MaxNumberForRepaireToDisk = iniReader.Int64("FirstMQ", "MaxNumberForRepaireToDisk")
	FirstMQConfig.FillNumberEachTime = iniReader.Int64("FirstMQ", "FillNumberEachTime")
	FirstMQConfig.IdleSleepTimeForWrite = time.Duration(iniReader.Int("FirstMQ", "IdleSleepTimeForWrite")) * time.Millisecond
	FirstMQConfig.IdleSleepTimeForRead = time.Duration(iniReader.Int("FirstMQ", "IdleSleepTimeForRead")) * time.Millisecond

}
