package kernel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/cnlesscode/firstMQServer/config"
	"github.com/cnlesscode/gotool"
	"github.com/cnlesscode/gotool/gfs"
)

// 话题列表
var TopicList = make(map[string]int)

// 获取话题列表
func GetTopicList() []string {
	topicList := make([]string, 0)
	for k := range TopicList {
		topicList = append(topicList, k)
	}
	return topicList
}

// 载入话题
func LoadTopics() {
	// 以数据文件夹为基础初始化话题管道
	fmt.Printf("config.FirstMQConfig.DataDir: %v\n", config.FirstMQConfig.DataDir)
	list, err := os.ReadDir(config.FirstMQConfig.DataDir)
	if err != nil {
		err = os.Mkdir(config.FirstMQConfig.DataDir, 0777)
		if err != nil {
			panic("✘ 话题数据文件夹创建失败！")
		}
		list, err = os.ReadDir(config.FirstMQConfig.DataDir)
		if err != nil {
			panic("✘ 话题数据文件夹初始化失败！")
		}
	}
	for _, fsd := range list {
		// 文件夹判断，排除文件
		if !fsd.IsDir() {
			continue
		}
		topicName := fsd.Name()
		if len(topicName) < 1 {
			continue
		}
		TopicList[topicName] = 1
		// 话题数据临时存储 Map
		if _, ok := MessageChannels[topicName]; ok {
			continue
		}
		MessageChannels[topicName] = make(chan []byte, config.FirstMQConfig.ChannelCapactiyForProduct)
		// 1. 启动落盘函数
		go SaveMessageToDisk(topicName)
		// 2. 启动消费消息缓冲管道填充
		// 2.1 遍历消费者组循环填充
		// 2.2 同时开启消费索引保存功能
		FillMessagesToConsumeChannel(topicName)
	}
	// 记录日志
	log.Println("✔ FirstMQ : 话题初始化成功")
}

// 创建话题
func CreateTopic(topicName string) error {
	log.Println("✔ FirstMQ : 创建话题 : " + topicName)
	err := CreateTopicFiles(topicName)
	if err != nil {
		return err
	}
	// 动态添加话题数据通道
	MessageChannels[topicName] = make(chan []byte, config.FirstMQConfig.ChannelCapactiyForProduct)
	// 动态添加话题列表数据
	TopicList[topicName] = 1
	// 创建消费者组
	// 01. 创建后自动开启消费管道填充
	// 02. 创建后自动开启消费索引保存
	err = CreateConsumerGroup(topicName, "default")
	if err != nil {
		return err
	}
	// 执行话题落盘
	go SaveMessageToDisk(topicName)
	return nil
}

// 检查话题名称
func CheckTopicName(topic string) error {
	reg, _ := regexp.Compile("^[a-z_0-9]+$")
	res := reg.MatchString(topic)
	if res {
		return nil
	}
	return errors.New("话题名称只能使用小写字母、数字和下划线")
}

// 创建话题目录及基础文件
func CreateTopicFiles(topicName string) error {
	err := CheckTopicName(topicName)
	if err != nil {
		return err
	}
	topicDataDir := config.FirstMQConfig.DataDir + gotool.SystemSeparator + topicName
	// 已经存在的话题
	if gfs.DirExists(topicDataDir) {
		return errors.New("话题已存在")
	}
	// 1. 创建落盘文件夹
	if !gfs.DirExists(config.FirstMQConfig.DataDir) {
		err := os.Mkdir(config.FirstMQConfig.DataDir, 0777)
		if err != nil {
			return err
		}
	}

	// 2. 创建话题目录
	err = os.Mkdir(topicDataDir, 0777)
	if err != nil {
		return err
	}

	// 3. 创建话题数据目录
	err = os.Mkdir(topicDataDir+gotool.SystemSeparator+"data_logs", 0777)
	if err != nil {
		return err
	}

	// 4. 创建存储索引记录文件
	f, err := os.OpenFile(topicDataDir+gotool.SystemSeparator+"save_index.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	err = binary.Write(f, binary.LittleEndian, int64(0))
	if err != nil {
		f.Close()
		return err
	}
	f.Close()

	// 5. 创建消费索引目录
	err = os.Mkdir(topicDataDir+gotool.SystemSeparator+"consume_logs", 0777)
	if err != nil {
		return err
	}
	return nil
}
