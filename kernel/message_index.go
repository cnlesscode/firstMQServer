package kernel

import (
	"encoding/binary"
	"os"
	"strconv"
	"time"

	"github.com/cnlesscode/firstMQServer/config"
	"github.com/cnlesscode/gotool"
)

// 读取存储索引 [ 当前存储最大值 - 整数 ]
func GetSaveIndex(topicName string) (int64, error) {
	saveIndexFilePath := config.FirstMQConfig.DataDir + gotool.SystemSeparator + topicName + gotool.SystemSeparator + "save_index.bin"
	f, err := os.OpenFile(saveIndexFilePath, os.O_RDONLY, 0777)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var saveIndex int64 = 0
	err = binary.Read(f, binary.LittleEndian, &saveIndex)
	if err != nil {
		return 0, err
	}
	return saveIndex, nil
}

// 整合消费索引 map 键名
func InitConsumeIndexMapKey(topicName, consumerGroup string) string {
	return topicName + "_" + consumerGroup
}

// 整合消费索引文件路径
func InitConsumeIndexFilePath(topicName, consumerGroup string) (string, string) {
	baseDataDir := config.FirstMQConfig.DataDir + gotool.SystemSeparator +
		topicName + gotool.SystemSeparator +
		"consume_logs" + gotool.SystemSeparator
	filePath := consumerGroup
	return baseDataDir, filePath
}

// 整合消息数据文件路径
// 返回 : 日志文件路径, 消息索引文件路径, 日志文件所在目录
func InitLogFiles(topicName string, fileIdx int64) (string, string) {
	baseDataDir := config.FirstMQConfig.DataDir + gotool.SystemSeparator + topicName + gotool.SystemSeparator
	logfilePath := baseDataDir + "data_logs" + gotool.SystemSeparator + strconv.FormatInt(fileIdx, 10) + ".log"
	idxFilePath := baseDataDir + "data_logs" + gotool.SystemSeparator + strconv.FormatInt(fileIdx, 10) + ".bin"
	return logfilePath, idxFilePath
}

// 获取消息索引
func GetMessageOffsetIntValue(offset string) (int64, error) {
	offsetLength := len(offset)
	intValStart := 0
	for i := 0; i < offsetLength; i++ {
		if offset[i:i+1] != "0" {
			intValStart = i
			break
		}
	}
	intValString := offset[intValStart:]
	intVal, err := strconv.ParseInt(intValString, 10, 64)
	return intVal, err
}

// 保存消费索引到文件
func (m *ConsumeMessagesChannel) SaveConsumeIndexToFile() {
	go func(mIn *ConsumeMessagesChannel) {
		for {
			// 读取索引检查是否有变化
			consumeIndexInFile, err := mIn.GetConsumeIndex()
			if err != nil {
				time.Sleep(time.Millisecond * 200)
				continue
			}
			if consumeIndexInFile == mIn.ConsumeIndex {
				time.Sleep(time.Millisecond * 200)
				continue
			}
			f, err := os.OpenFile(mIn.ConsumeIndexFilePath, os.O_WRONLY, 0777)
			if err != nil {
				time.Sleep(time.Millisecond * 200)
				continue
			}
			binary.Write(f, binary.LittleEndian, mIn.ConsumeIndex)
			f.Close()
			time.Sleep(time.Millisecond * 200)
		}
	}(m)
}
