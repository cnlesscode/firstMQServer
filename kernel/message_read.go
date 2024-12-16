package kernel

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/cnlesscode/firstMQServer/config"
)

type MessageForRead struct {
	Data  []byte
	Index int64
}

// 从日志文件中读取消息
// 参数 : 主题名称, 消费者组名称, 起始索引, 要读取的消息内容长度
// 返回 : 消息内容 [][]byte, 消息数量, 错误
func ReadMessages(
	topicName, consumerGroup string,
	startIndex, readLength int64) ([]MessageForRead, int64, error) {
	// 消息区间距离不能跨域2个分片
	if readLength > config.FirstMQConfig.NumberOfFragmented {
		return nil, 0, errors.New("长度超过分片容量")
	}
	if readLength < 1 {
		readLength = 1
	}
	// 目标索引
	targetIndex := startIndex + readLength
	// 模板索引不能超过最大存储索引
	saveIndex, err := GetSaveIndex(topicName)
	if err != nil {
		return nil, 0, errors.New("获取存储索引失败")
	}
	if targetIndex > saveIndex {
		targetIndex = saveIndex
	}
	messageCountNumber := targetIndex - startIndex
	if messageCountNumber < 1 {
		return nil, 0, errors.New("暂无新的消息")
	}

	// 1. 获取索引
	dataLogPaths, messageOffsetMap, err := GetMessageIndexData(topicName, startIndex, messageCountNumber)
	if err != nil {
		return nil, 0, errors.New("获取索引数据失败")
	}
	messages := make([]MessageForRead, 0)
	for k, dataLogPath := range dataLogPaths {
		f, err := os.OpenFile(dataLogPath, os.O_RDONLY, 0666)
		for _, offsets := range messageOffsetMap[k] {
			if err != nil {
				messages = append(messages, MessageForRead{Index: startIndex, Data: nil})
			}
			// 从文件中读取消息
			reader := io.NewSectionReader(f, offsets.Start, offsets.Length)
			buffer := make([]byte, offsets.Length)
			_, err := reader.Read(buffer)
			if err != nil {
				messages = append(messages, MessageForRead{Index: startIndex, Data: nil})
			}
			messages = append(messages, MessageForRead{Index: startIndex, Data: buffer})
			startIndex++
		}
	}
	return messages, messageCountNumber, nil

}

// 获取消息日志索引位置数据及相关文件
// 参数 : 主题名称, 消息索引
// 返回 : 数据文件路径, 消息索引数据, 错误

type MessageOffsetStruct struct {
	Start  int64
	Length int64
}

func GetMessageIndexData(topicName string, startIndex, length int64) ([]string, map[int][]MessageOffsetStruct, error) {

	indexData := make(map[int][]MessageOffsetStruct, 0)
	dataLogPaths := make([]string, 0)

	// 根据 offset 计算索引及数据文件位置
	startFileIdex := startIndex / config.FirstMQConfig.NumberOfFragmented

	// 检查消息是否位于2个分片中
	endIndex := startIndex + length
	endFileIdex := endIndex / config.FirstMQConfig.NumberOfFragmented

	// 同一个分片
	if startFileIdex == endFileIdex {
		logFilePath, indexFilePath := InitLogFiles(topicName, startFileIdex)
		dataLogPaths = append(dataLogPaths, logFilePath)
		indexData[0] = make([]MessageOffsetStruct, 0)
		// 打开数据索引文件
		indexFile, err := os.OpenFile(indexFilePath, os.O_RDONLY, 0777)
		if err != nil {
			return nil, nil, err
		}
		defer indexFile.Close()
		startIndexFor := startIndex - startFileIdex*config.FirstMQConfig.NumberOfFragmented
		for i := int64(0); i < length; i++ {
			indexFile.Seek((startIndexFor+i)*16, 0)
			var offsetStart int64 = 0
			binary.Read(indexFile, binary.LittleEndian, &offsetStart)
			var Length int64 = 0
			binary.Read(indexFile, binary.LittleEndian, &Length)
			indexData[0] = append(indexData[0], MessageOffsetStruct{Start: offsetStart, Length: Length})
		}
		return dataLogPaths, indexData, nil
	}

	// 2个分先读取第一个
	logFilePath, indexFilePath := InitLogFiles(topicName, startFileIdex)
	dataLogPaths = append(dataLogPaths, logFilePath)
	indexData[0] = make([]MessageOffsetStruct, 0)
	// 打开数据索引文件
	indexFile, err := os.OpenFile(indexFilePath, os.O_RDONLY, 0777)
	if err != nil {
		return nil, nil, err
	}
	endLengthForFirst := config.FirstMQConfig.NumberOfFragmented*(startFileIdex+1) - startIndex
	startForFirst := startIndex - startFileIdex*config.FirstMQConfig.NumberOfFragmented
	for i := int64(0); i < endLengthForFirst; i++ {
		indexFile.Seek((i+startForFirst)*16, 0)
		var offsetStart int64 = 0
		binary.Read(indexFile, binary.LittleEndian, &offsetStart)
		var Length int64 = 0
		binary.Read(indexFile, binary.LittleEndian, &Length)
		indexData[0] = append(indexData[0], MessageOffsetStruct{Start: offsetStart, Length: Length})
	}
	indexFile.Close()

	// 刚好是第一个文件的结束点
	if endLengthForFirst == length {
		return dataLogPaths, indexData, nil
	}

	// 再读取第2个文件
	logFilePath, indexFilePath = InitLogFiles(topicName, endFileIdex)
	dataLogPaths = append(dataLogPaths, logFilePath)
	indexData[1] = make([]MessageOffsetStruct, 0)
	// 打开数据索引文件
	indexFile2, err := os.OpenFile(indexFilePath, os.O_RDONLY, 0777)
	if err != nil {
		return dataLogPaths, indexData, nil
	}
	endLengthForSecond := endIndex - config.FirstMQConfig.NumberOfFragmented*endFileIdex
	for i := int64(0); i < endLengthForSecond; i++ {
		indexFile2.Seek(i*16, 0)
		var offsetStart int64 = 0
		binary.Read(indexFile2, binary.LittleEndian, &offsetStart)
		var Length int64 = 0
		binary.Read(indexFile2, binary.LittleEndian, &Length)
		indexData[1] = append(indexData[1], MessageOffsetStruct{Start: offsetStart, Length: Length})
	}
	indexFile2.Close()
	return dataLogPaths, indexData, nil
}
