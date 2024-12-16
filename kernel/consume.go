package kernel

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/cnlesscode/firstMQServer/config"
	"github.com/cnlesscode/gotool/gfs"
)

// 消费话题管道
type ConsumeMessagesChannel struct {
	TopicName            string
	ConsumerGroup        string
	Channel              chan MessageForRead
	FillIndex            int64
	ConsumeIndexFilePath string
	ConsumeIndex         int64
}

// 消费消息
func Consume(topicName string, consumerGroup string) ([]byte, error) {
	// 消息缓存通道键名称
	keyName := InitConsumeIndexMapKey(topicName, consumerGroup)
	_, ok := ConsumeMessageChannels[keyName]
	// 不存在则创建
	if !ok {
		return nil, errors.New("话题或消费者组不存在")
	}
	// 从缓存通道获取消息
	select {
	case message := <-ConsumeMessageChannels[keyName].Channel:
		ConsumeMessageChannels[keyName].ConsumeIndex = message.Index + 1
		return message.Data, nil
	default:
		return nil, errors.New("no message")
	}
}

// 创建消费者组
func CreateConsumerGroup(topicName, consumerGroup string) error {
	// 检查话题合理性
	if _, ok := TopicList[topicName]; !ok {
		return errors.New("话题 " + topicName + "不存在")
	}
	// 名称检查
	if err := CheckTopicName(consumerGroup); err != nil {
		return errors.New("话题 " + topicName + "名称错误")
	}
	// 组合键名称
	consumeIndexMapKey := InitConsumeIndexMapKey(topicName, consumerGroup)
	// 判断消费者组是否已经存在
	if _, ok := ConsumeMessageChannels[consumeIndexMapKey]; ok {
		return nil
	}
	// 消费索引
	dir, fName := InitConsumeIndexFilePath(topicName, consumerGroup)
	consumeIndexFilePath := dir + fName
	if gfs.FileExists(consumeIndexFilePath) {
		return nil
	}
	//
	f, err := os.OpenFile(consumeIndexFilePath, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	err = binary.Write(f, binary.LittleEndian, int64(0))
	f.Close()
	if err != nil {
		return err
	}
	ConsumeMessageChannels[consumeIndexMapKey] = &ConsumeMessagesChannel{
		TopicName:            topicName,
		Channel:              make(chan MessageForRead, config.FirstMQConfig.FillNumberEachTime-1),
		ConsumerGroup:        consumerGroup,
		ConsumeIndexFilePath: consumeIndexFilePath,
		FillIndex:            0,
		ConsumeIndex:         0,
	}
	// FillMessages 填充消息同时会开启消费索引保存功能 ( 一个子协程 )
	ConsumeMessageChannels[consumeIndexMapKey].FillMessages()
	return nil
}
