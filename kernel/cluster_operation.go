package kernel

import (
	"encoding/json"
	"net"
	"time"

	"github.com/cnlesscode/firstKV"
	"github.com/cnlesscode/firstMQServer/config"
	"github.com/cnlesscode/gotool"
)

// 在各个集群节点上创建话题
func CreateTopicForClusters(topicName string) error {
	// 获取集群节点
	nodes, err := GetClusterNodes()
	if err != nil {
		return err
	}
	// 通知集群节点添加消费组
	for _, node := range nodes {
		conn, err := net.DialTimeout("tcp", node.Addr, time.Second*5)
		if err != nil {
			continue
		}
		defer conn.Close()
		message := SendMessageStruct{
			Action: 9,
			Topic:  topicName,
		}
		SendMessage(conn, message)
	}
	return nil
}

// 在各个集群节点上创建消费组
func CreateConsumeGroupForClusters(topicName, consumerGroup string) error {
	// 获取集群节点
	nodes, err := GetClusterNodes()
	if err != nil {
		return err
	}
	// 通知集群节点添加消费组
	for _, node := range nodes {
		conn, err := net.DialTimeout("tcp", node.Addr, time.Second*5)
		if err != nil {
			continue
		}
		defer conn.Close()
		message := SendMessageStruct{
			Action:        8,
			Topic:         topicName,
			ConsumerGroup: consumerGroup,
		}
		SendMessage(conn, message)
	}
	return nil
}

// 获取集群服务器列表
func GetClusterNodes() (firstKV.FirstMQAddrs, error) {
	nodes := firstKV.FirstMQAddrs{}
	conn, err := net.DialTimeout("tcp", config.MasterIP+":"+config.FirstKVConfig.Port, time.Second*5)
	if err != nil {
		return nodes, err
	}
	defer conn.Close()
	message := firstKV.ReceiveMessage{
		Action: "get mqServers",
		Key:    "firstMQServers",
		Data:   firstKV.FirstMQAddr{},
	}
	response, err := firstKV.Send(conn, message)
	if err != nil {
		return nodes, err
	}

	err = json.Unmarshal([]byte(response.Data), &nodes)
	if err != nil {
		return nodes, err
	}
	return nodes, nil
}

// 发送消息
func SendMessage(conn net.Conn, message SendMessageStruct) (ResponseMessage, error) {
	defer conn.Close()
	response := ResponseMessage{}
	msgByte, _ := json.Marshal(message)
	err := gotool.WriteTCPResponse(conn, msgByte)
	if err != nil {
		return response, err
	}
	buf, err := gotool.ReadTCPResponse(conn)
	if err != nil {
		return response, err
	}
	err = json.Unmarshal(buf, &response)
	if err != nil {
		return response, err
	}
	return response, nil
}
