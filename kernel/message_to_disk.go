package kernel

import (
	"encoding/binary"
	"log"
	"os"
	"path"
	"time"

	"github.com/cnlesscode/firstMQServer/configs"
)

// 落盘函数
func SaveMessageToDisk(topicName string) {
	log.Println("✔ FirstMQ : 开始落盘，话题:" + topicName)
SaveMessageToDiskStart:
	// 遍历话题内消息
	var messageCount int64 = 0
	// 从管道内取出消息
	messageSlice := make([][]byte, 0)
	for {
		getedValue := false
		select {
		case message := <-MessageChannels[topicName]:
			messageSlice = append(messageSlice, message)
			getedValue = true
		default:
			getedValue = false
		}
		if getedValue {
			messageCount++
			if messageCount >= configs.FirstMQConfig.MaxNumberForRepaireToDisk {
				break
			}
		} else {
			if messageCount > 0 {
				break
			} else {
				// 此处延迟时间越短 CPU 占用越高
				// 主机运算速度快应该设置大一些，否则循环会占用 CPU
				// 经测试 50 ~ 100 毫秒比较合理
				time.Sleep(configs.FirstMQConfig.IdleSleepTimeForWrite)
			}
		}
	}
	// 获取存储索引
	saveIndexFilePath := path.Join(
		configs.FirstMQConfig.DataDir,
		topicName,
		"save_index.bin",
	)
	saveIndex, err := GetSaveIndex(topicName)
	if err != nil {
		SaveMessageToDiskFailed(topicName, messageSlice)
		goto SaveMessageToDiskStart
	}
	// 情况 1 : 第一次落盘 0.log
	if saveIndex == 0 {
		logFilePath, indxFilePath := InitLogFiles(topicName, 0)
		f, err := os.Create(logFilePath)
		if err != nil {
			SaveMessageToDiskFailed(topicName, messageSlice)
			goto SaveMessageToDiskStart
		}
		f.Close()
		f, err = os.Create(indxFilePath)
		if err != nil {
			SaveMessageToDiskFailed(topicName, messageSlice)
			goto SaveMessageToDiskStart
		}
		f.Close()
		_, err = SaveMessageToDiskBase(
			messageSlice,
			0,
			messageCount,
			0,
			0,
			logFilePath,
			indxFilePath,
			saveIndexFilePath,
		)
		if err != nil {
			// 落盘失败回滚消息
			SaveMessageToDiskFailed(topicName, messageSlice)
			goto SaveMessageToDiskStart
		}
		// 成功 : 继续落盘
		goto SaveMessageToDiskStart
	}

	// 情况 2 : 非第一次落盘
	// 通过数据长度及存储索引决定数据文件分配
	fileIndex := saveIndex / configs.FirstMQConfig.NumberOfFragmented
	// 当前数据文件剩余条目数
	fileResidueNumber := (fileIndex+1)*configs.FirstMQConfig.NumberOfFragmented - saveIndex

	// 情况 2.1 : 当前分片有剩余空间
	if fileResidueNumber > 0 {
		var maxStep int64 = 0
		if fileResidueNumber >= messageCount {
			maxStep = messageCount
		} else {
			maxStep = fileResidueNumber
		}
		logFilePath, indxFilePath := InitLogFiles(topicName, fileIndex)
		// 新的分片第一次落盘
		var startOffset int64 = 0
		if fileResidueNumber == configs.FirstMQConfig.NumberOfFragmented {
			startOffset = 0
		} else {
			fi, err := os.Stat(logFilePath)
			if err != nil {
				SaveMessageToDiskFailed(topicName, messageSlice)
				goto SaveMessageToDiskStart
			}
			startOffset = fi.Size()
		}
		// 落盘当前分片
		saveIndex, err = SaveMessageToDiskBase(
			messageSlice,
			0,
			maxStep,
			saveIndex,
			startOffset,
			logFilePath,
			indxFilePath,
			saveIndexFilePath,
		)
		if err != nil {
			SaveMessageToDiskFailed(topicName, messageSlice)
			goto SaveMessageToDiskStart
		}
		// 落盘成功继续落盘
		if messageCount <= fileResidueNumber {
			goto SaveMessageToDiskStart
		} else {
			// 情况 2.2 : 当前分片存满后还有剩余的数据
			// 新建一个分片存储剩余数据
			messageSlice = messageSlice[maxStep:]
			messageCount = int64(len(messageSlice))
			logFilePath, indxFilePath := InitLogFiles(topicName, fileIndex+1)
			_, err = SaveMessageToDiskBase(
				messageSlice,
				0,
				messageCount,
				saveIndex,
				0,
				logFilePath,
				indxFilePath,
				saveIndexFilePath,
			)
			if err != nil {
				// 落盘失败回滚消息
				SaveMessageToDiskFailed(topicName, messageSlice)
			}
			// 成功 : 继续落盘
			goto SaveMessageToDiskStart
		}

	} else {
		// 情况3 : 分片没有剩余新建新的分片
		logFilePath, indxFilePath := InitLogFiles(topicName, fileIndex+1)
		_, err = SaveMessageToDiskBase(
			messageSlice,
			0,
			messageCount,
			saveIndex,
			0,
			logFilePath,
			indxFilePath,
			saveIndexFilePath,
		)
		if err != nil {
			// 落盘失败回滚消息
			SaveMessageToDiskFailed(topicName, messageSlice)
		}
		// 成功 : 继续落盘
		goto SaveMessageToDiskStart
	}
}

// 落盘基础函数
func SaveMessageToDiskBase(
	messages [][]byte,
	forStart,
	forEnd int64,
	saveIndex int64,
	startOffet int64,
	logFile,
	indexFile,
	saveIndexFilePath string,
) (int64, error) {
	// 循环组合消息内容
	dataPrepare := make([]byte, 0)
	var messageCount int64 = 0
	indexData := make([]int64, 0)
	for forStart < forEnd {
		msgBytes := messages[messageCount]
		dataPrepare = append(dataPrepare, msgBytes...)
		indexData = append(indexData, startOffet)
		dataLen := int64(len(msgBytes))
		indexData = append(indexData, dataLen)
		startOffet += dataLen
		forStart++
		messageCount++
	}

	// 1. 存储消息数据
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	if err != nil {
		return saveIndex, err
	}

	defer file.Close()
	_, err = file.Write(dataPrepare)
	if err != nil {
		return saveIndex, err
	}

	// 2. 存储索引数据
	messageIndexFile, err := os.OpenFile(indexFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	if err != nil {
		return saveIndex, err
	}
	// 2.1 修正存储索引
	indexFileStatus, err := messageIndexFile.Stat()
	if err != nil {
		return saveIndex, err
	}
	indexFileSize := indexFileStatus.Size()
	countedSaveIndex := indexFileSize / 16
	saveIndexCurrent := saveIndex % configs.FirstMQConfig.NumberOfFragmented
	if countedSaveIndex != saveIndexCurrent {
		saveIndex = saveIndex + (countedSaveIndex - saveIndexCurrent)
	}
	// 2.2 写入索引数据
	err = binary.Write(messageIndexFile, binary.LittleEndian, indexData)
	if err != nil {
		return saveIndex, err
	}

	// 3. 记录 saveIndex
	saveIndex += messageCount
	saveIndexFile, err := os.OpenFile(saveIndexFilePath, os.O_RDWR, 0777)
	if err != nil {
		return saveIndex, err
	}
	binary.Write(saveIndexFile, binary.LittleEndian, saveIndex)
	return saveIndex, nil
}

// 落盘失败
func SaveMessageToDiskFailed(topicName string, messages [][]byte) {
	go func() {
		for _, message := range messages {
			MessageChannels[topicName] <- message
		}
	}()
}
