package server

import (
	"log"
	"net"

	"github.com/cnlesscode/firstMQServer/configs"
	"github.com/cnlesscode/firstMQServer/kernel"
	"github.com/cnlesscode/gotool"
)

// TCPServer TCP服务器结构
type TCPServer struct {
	listener net.Listener
}

// 创建TCP服务器
func NewTCPServer(addr string) *TCPServer {
	// 创建 Socket 端口监听
	// listener 是一个用于面向流的网络协议的公用网络监听器接口，
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	// 返回实例
	return &TCPServer{listener: listener}
}

// Accept 等待客户端连接
func (t *TCPServer) Accept() {
	// 处理客户端连接
	// 关闭接口解除阻塞的 Accept 操作并返回错误
	defer t.listener.Close()
	// 循环等待客户端连接
	for {
		// 等待客户端连接
		conn, err := t.listener.Accept()
		if err == nil {
			go t.Handle(conn)
		}
	}
}

// Handle 处理客户端连接
func (t *TCPServer) Handle(conn net.Conn) {
	for {
		content, err := gotool.ReadTCPResponse(conn)
		if err != nil {
			conn.Close()
			break
		}
		// 输出响应
		// 响应消息长度
		err = gotool.WriteTCPResponse(conn, TCPResponse(content))
		if err != nil {
			conn.Close()
			break
		}
	}
}

// 开启 TCP 服务
func StartFirstMQTcpServer() {
	// 1. 注册服务到 ServerFinder
	kernel.RegisterFirstMQService()
	// 2. 初始化 FirstMQ 话题
	kernel.LoadTopics()
	// 3. 启动 FirstMQ TCP 服务
	tcpServer := NewTCPServer(":" + configs.FirstMQConfig.Port)
	log.Println("✔ FirstMQ : 服务" + "启动成功，端口:" + configs.FirstMQConfig.Port)
	tcpServer.Accept()
}
