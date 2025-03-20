package server

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/cnlesscode/firstMQServer/config"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// 接收消息结构体
type HttpReceiveMessage struct {
	Action        int
	Topic         string
	Data          string
	ConsumerGroup string
}

func StartWSServer() {

	// 初始化 Gin
	gin.SetMode(gin.ReleaseMode)
	var ge *gin.Engine = gin.New()

	// 允许跨域
	ge.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"*"},
		AllowHeaders:     []string{"*"},
		ExposeHeaders:    []string{"*"},
		AllowCredentials: true,
		AllowWildcard:    true,
		MaxAge:           12 * time.Hour,
	}))

	// 可信任代理
	ge.SetTrustedProxies([]string{"*"})

	// 提升 HTTP 服务为 websocket 服务
	ge.GET("/", func(ctx *gin.Context) {
		var upgrader = websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		}
		// 开始服务为 websocket
		conn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			log.Println("✘ 升级 http 到 websocket 服务失败")
			return
		}
		// 接收消息
		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				break
			}
			messageTmp := HttpReceiveMessage{}
			err = json.Unmarshal(message, &messageTmp)
			if err != nil {
				conn.WriteMessage(messageType, ResponseResult(100002, "数据格式错误", 0))
			}
			messageForKernel := ReceiveMessage{
				Action:        messageTmp.Action,
				ConsumerGroup: messageTmp.ConsumerGroup,
				Data:          []byte(messageTmp.Data),
				Topic:         messageTmp.Topic,
			}
			message, _ = json.Marshal(messageForKernel)
			responseMessage := TCPResponse(message)
			conn.WriteMessage(messageType, responseMessage)
		}
	})

	// 启动服务
	log.Println("✔ FirstMQ : 服务" + "启动成功，端口:" + config.FirstMQConfig.HTTPPort)
	ge.Run(":" + config.FirstMQConfig.HTTPPort)

}
