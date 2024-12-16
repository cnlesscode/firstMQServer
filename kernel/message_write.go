package kernel

import (
	"errors"
)

// 写入消息到管道中
func WriteMessage(topicName string, data []byte) error {
	ch, ok := MessageChannels[topicName]
	if !ok {
		return errors.New("消息对应话题错误")
	}
	ch <- data
	return nil
}
