package main

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go"
	"github.com/preceeder/go/bnats"
	"log"
	"strconv"
	"time"
)

type MyMessage struct {
	ID       int    `json:"id"`
	Content  string `json:"content"`
	Topic    string `json:"topic"`
	SendTime string `json:"send_time"`
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL) // 默认: nats://localhost:4222
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// 发布带延迟的 JSON 消息
	msgObj := MyMessage{
		ID:       1,
		Content:  "Hello with delay",
		Topic:    "alerts.delayed",
		SendTime: time.Now().Format(time.DateTime),
	}
	data, _ := json.Marshal(msgObj)

	natsMsg := nats.NewMsg("alerts.delayed")
	natsMsg.Data = data
	natsMsg.Header.Set("Nats-Msg-Id", strconv.FormatInt(time.Now().Unix(), 10))
	natsMsg.Header.Set("Reply-Subject", "alerts.delayed")
	natsMsg.Header.Set("Delay-Time", "5") // 延迟 5 秒发送
	_, err = js.PublishMsg(natsMsg)
	if err != nil {
		log.Fatal("Publish failed:", err)
	}
	log.Println("Published delayed message")

}

var jt *bnats.JetStreamConsumer

func init() {
	jt = bnats.NewJetStreamConsumer(nats.DefaultURL)
}

func PublicData() {
	ctx := context.Background()
	// 发布带延迟的 JSON 消息
	msgObj := MyMessage{
		ID:       1,
		Content:  "Hello with delay",
		Topic:    "alerts.delayed.re",
		SendTime: time.Now().Format(time.DateTime),
	}
	data, _ := json.Marshal(msgObj)

	natsMsg := nats.NewMsg("alerts.sde.re")
	natsMsg.Data = data
	natsMsg.Header.Set("Delay-Time", "5") // 延迟 5 秒发送
	datr := jt.PublishMsg(ctx, natsMsg)

	log.Println("Published delayed message", datr)
}

// rpc模式
func Request() {
	ctx := context.Background()
	// 发布带延迟的 JSON 消息
	msgObj := MyMessage{
		ID:       1,
		Content:  "Hello with delay",
		Topic:    "alerts.delayed.re",
		SendTime: time.Now().Format(time.DateTime),
	}
	data, _ := json.Marshal(msgObj)
	datr, _ := jt.Request(ctx, "request-test", data, time.Second*2, nil)

	log.Println("Published delayed message", string(datr))
}
