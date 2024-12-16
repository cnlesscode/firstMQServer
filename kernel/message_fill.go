package kernel

import (
	"encoding/binary"
	"os"
	"strings"
	"time"

	"github.com/cnlesscode/firstMQServer/config"
	"github.com/cnlesscode/gotool"
)

// 填充全部消费队列
func FillMessagesToConsumeChannel(topicName string) {
	// 获取消费者组
	for topicName := range TopicList {
		baseDataDir := config.FirstMQConfig.DataDir + gotool.SystemSeparator +
			topicName + gotool.SystemSeparator +
			"consume_logs" + gotool.SystemSeparator
		// 查询消费者组
		fileList, err := os.ReadDir(baseDataDir)
		if err != nil {
			return
		}
		for _, indexFile := range fileList {
			fileName := indexFile.Name()
			if !strings.Contains(fileName, ".") {
				// 填充管道
				key := InitConsumeIndexMapKey(topicName, fileName)
				_, ok := ConsumeMessageChannels[key]
				if !ok {
					// 索引文件
					dir, fName := InitConsumeIndexFilePath(topicName, fileName)
					consumeIndexFilePath := dir + fName
					ConsumeMessageChannels[key] = &ConsumeMessagesChannel{
						TopicName:            topicName,
						ConsumerGroup:        fileName,
						Channel:              make(chan MessageForRead, config.FirstMQConfig.FillNumberEachTime-1),
						ConsumeIndexFilePath: consumeIndexFilePath,
					}
					startIndex, err := ConsumeMessageChannels[key].GetConsumeIndex()
					if err != nil {
						continue
					}
					if startIndex < 0 {
						ConsumeMessageChannels[key].FillIndex = 0
					} else {
						ConsumeMessageChannels[key].FillIndex = startIndex
					}

					ConsumeMessageChannels[key].ConsumeIndex = startIndex
					// FillMessages 填充消息同时会开启消费索引保存功能 ( 一个子协程 )
					ConsumeMessageChannels[key].FillMessages()
				}
			}
		}
	}
}

func (m *ConsumeMessagesChannel) FillMessages() {
	go func(mIn *ConsumeMessagesChannel) {
		for {
			messages, messageCount, err := ReadMessages(
				mIn.TopicName,
				mIn.ConsumerGroup,
				mIn.FillIndex,
				config.FirstMQConfig.FillNumberEachTime)
			if err != nil {
				// 测试分片消费 及 计算获取消息的开销
				time.Sleep(config.FirstMQConfig.IdleSleepTimeForRead)
				continue
			}
			// 将消息填充到消费者消息缓存通道
			// 当消费者缓存通道满时，阻塞等待
			for _, message := range messages {
				mIn.Channel <- message
				// 更新消费索引
			}
			mIn.FillIndex += messageCount
		}
	}(m)
	// 循环保存消费索引
	m.SaveConsumeIndexToFile()
}

func (m *ConsumeMessagesChannel) GetConsumeIndex() (int64, error) {
	f, err := os.OpenFile(m.ConsumeIndexFilePath, os.O_RDONLY, 0666)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var consumeIndex int64 = 0
	err = binary.Read(f, binary.LittleEndian, &consumeIndex)
	if err != nil {
		return 0, err
	}
	return consumeIndex, nil
}
