package server

import (
	"encoding/json"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cnlesscode/firstMQServer/kernel"
)

// 消息类型
const (
	Product                    int = 1
	Consume                    int = 2
	CreateTopic                int = 3
	TopicList                  int = 4
	ServerStatus               int = 5
	Ping                       int = 6
	CreateConsumeGroup         int = 7
	CreateConsumeGroupForLocal int = 8
	CreateTopicForLocal        int = 9
	SeverList                  int = 10
	SeverStatus                int = 11
	SeverStatusLocal           int = 12
)

// 接收消息结构体
type ReceiveMessage struct {
	Action        int
	Topic         string
	Data          []byte
	ConsumerGroup string
}

// 响应消息结构体
type ResponseMessage struct {
	ErrCode int
	Data    string
	Type    int
}

// TCP 服务响应函数
func TCPResponse(msg []byte) []byte {
	// 将消息转换为结构体
	message := &ReceiveMessage{}
	err := json.Unmarshal(msg, message)
	if err != nil {
		return ResponseResult(100001, "消息格式错误", 0)
	}
	return Response(message)
}

// 响应函数
func Response(message *ReceiveMessage) []byte {
	switch {
	// 生产消息
	case message.Action == Product:
		if message.Topic == "" {
			return ResponseResult(100002, "话题数据错误", 0)
		}
		err := kernel.WriteMessage(message.Topic, message.Data)
		if err != nil {
			return ResponseResult(100051, err.Error(), 0)
		}
		return ResponseResult(0, "消息提交成功", Product)

	// 消费消息
	case message.Action == Consume:
		if message.Topic == "" {
			return ResponseResult(100002, "话题数据错误", 0)
		}
		if message.ConsumerGroup == "" {
			message.ConsumerGroup = "default"
		}
		messageIn, err := kernel.Consume(message.Topic, message.ConsumerGroup)
		if err != nil {
			return ResponseResult(100380, err.Error(), 0)
		}
		return ResponseResult(0, string(messageIn), Consume)

	// 创建话题 [ 集群 ]
	case message.Action == CreateTopic:
		err := kernel.CreateTopicForClusters(message.Topic)
		if err != nil {
			return ResponseResult(100301, err.Error(), 0)
		}
		return ResponseResult(0, "话题 : "+message.Topic+" 创建成功", CreateTopic)
	// 创建话题 [ 本机 ]
	case message.Action == CreateTopicForLocal:
		err := kernel.CreateTopic(message.Topic)
		if err != nil {
			return ResponseResult(100301, err.Error(), 0)
		}
		return ResponseResult(0, "话题 : "+message.Topic+" 创建成功", CreateTopicForLocal)

	// 创建消费者组 [本机]
	case message.Action == CreateConsumeGroupForLocal:
		err := kernel.CreateConsumerGroup(message.Topic, message.ConsumerGroup)
		if err != nil {
			return ResponseResult(100300, err.Error(), 0)
		}
		// 自己创建完毕后，通知集群其他服务器端，同步创建
		return ResponseResult(0, "消费者组 : "+message.ConsumerGroup+" 创建成功", CreateConsumeGroupForLocal)
	// 创建消费者组 [集群]
	case message.Action == CreateConsumeGroup:
		err := kernel.CreateConsumeGroupForClusters(message.Topic, message.ConsumerGroup)
		if err != nil {
			return ResponseResult(100301, err.Error(), 0)
		}
		return ResponseResult(0, "消费者组 : "+message.ConsumerGroup+" 创建成功", CreateConsumeGroup)

	// 查询话题列表
	case message.Action == TopicList:
		topics := kernel.GetTopicList()
		topicsbytes, _ := json.Marshal(topics)
		return ResponseResult(0, string(topicsbytes), TopicList)

	// 服务状态
	case message.Action == ServerStatus:
		// 获取服务器线程数
		numForGoroutine := runtime.NumGoroutine()
		// 消息内容
		msg := "当前服务状态 : 可用\n"
		msg += "当前协程数 : " + strconv.Itoa(numForGoroutine)
		return ResponseResult(0, msg, ServerStatus)

	// Ping
	case message.Action == Ping:
		return ResponseResult(0, "pong", Ping)

	// 服务器列表
	case message.Action == SeverList:
		// 获取集群节点
		nodes, _ := kernel.GetClusterNodes()
		nodesBytes, _ := json.Marshal(nodes)
		return ResponseResult(0, string(nodesBytes), SeverList)

	// 服务器状态 - 本机
	case message.Action == SeverStatusLocal:
		// 话题列表
		// 1. 消费者组信息
		// 2. 存储索引信息
		// 3. 服务器 cpu 使用情况
		data := make(map[string]map[string]any)
		topics := kernel.TopicList
		for k := range topics {
			data[k] = make(map[string]any)
			// 存储索引
			data[k]["saveIndex"], _ = kernel.GetSaveIndex(k)
			// 消费者组
			consumerGroups := make([]map[string]any, 0)
			for kc := range kernel.ConsumeMessageChannels {
				if strings.Contains(kc, k+"_") {
					consumerGroups = append(consumerGroups, map[string]any{
						"Name":         kernel.ConsumeMessageChannels[kc].ConsumerGroup,
						"ConsumeIndex": kernel.ConsumeMessageChannels[kc].ConsumeIndex,
						"FillIndex":    kernel.ConsumeMessageChannels[kc].FillIndex,
					})
				}
			}
			data[k]["consumerGroups"] = consumerGroups
		}
		dataByte, err := json.Marshal(data)
		if err != nil {
			return ResponseResult(100404, "服务器内部错误", 0)
		}
		return ResponseResult(0, string(dataByte), SeverStatusLocal)
	case message.Action == SeverStatus:
		nodes, err := kernel.GetClusterNodes()
		if err != nil {
			return ResponseResult(100401, "服务器列表获取失败", 0)
		}
		returnData := make(map[string]map[string]any, 0)
		for _, node := range nodes {
			conn, err := net.DialTimeout("tcp", node.Addr, time.Second*5)
			if err != nil {
				continue
			}
			defer conn.Close()
			message := kernel.SendMessageStruct{
				Action: 12,
			}
			res, err := kernel.SendMessage(conn, message)
			if err != nil {
				continue
			}
			resFormat := make(map[string]any)
			json.Unmarshal([]byte(res.Data), &resFormat)
			returnData[node.Addr] = resFormat
		}
		dataByte, err := json.Marshal(returnData)
		if err != nil {
			return ResponseResult(100402, "状态获取失败", 0)
		}
		return ResponseResult(0, string(dataByte), SeverStatus)

	// 默认响应
	default:
		return ResponseResult(100404, "消息类型错误", 0)
	}
}

// 响应结果
func ResponseResult(errcode int, data string, tp int) []byte {
	responseMessage := ResponseMessage{
		ErrCode: errcode,
		Data:    data,
		Type:    tp,
	}
	responseMessageByte, _ := json.Marshal(responseMessage)
	return responseMessageByte
}
