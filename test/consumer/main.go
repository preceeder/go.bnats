// consumer.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/preceeder/go/bnats"
	"log"
	"log/slog"
	"time"
)

var jt *bnats.JetStreamConsumer

func init() {
	jt = bnats.NewJetStreamConsumer(nats.DefaultURL)
}

func Consumer() {
	err := jt.AddStream(&nats.StreamConfig{
		Name:     "MESSAGES",
		Subjects: []string{"messages.*", "alerts.>"},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		slog.Error(err.Error())
		return
	}
	ctx := context.Background()
	for _, data := range QueryAllConsumerInfo("MESSAGES") {
		fmt.Println(*data)
	}
	//jt.DeleteConsumer(ctx, "MESSAGES", "pull-messsage")
	jt.AddConsumer(ctx, "MESSAGES", &nats.ConsumerConfig{
		Durable: "pull-messsage", // Durable 名称（必须用于 Bind） // 持久化的队列名
		// DeliverSubject, DeliverGroup 都是push模式特有的， 配置了就是push模式
		//DeliverSubject: "sonsumer.message.print", // FilterSubject 过滤原始的subject, 然后将消息转发到 DeliverSubject 设置的subject, 相当于设置副本
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverNewPolicy, // 只接收创建后最新的推送， 创建后重启过程中没有接收的消息在重启后会继续接收
		FilterSubjects: []string{"messages.*", "alerts.>"},
		MaxDeliver:     5,               // 最多尝试次数
		AckWait:        5 * time.Second, // 应答等待时间
	})

	jt.PullSubscribe(context.Background(), "alerts.>", "pull-messsage", 5, PullHandler)

	jt.AddConsumer(ctx, "MESSAGES", &nats.ConsumerConfig{
		Durable: "push-messsage", // Durable 名称（必须用于 Bind） // 持久化的队列名
		//DeliverSubject, DeliverGroup 都是push模式特有的， 配置了就是push模式
		DeliverSubject: "sonsumer.message.print", // FilterSubject 过滤原始的subject, 然后将消息转发到 DeliverSubject 设置的subject, 相当于设置副本
		DeliverGroup:   "sonsumer-queue",         // queue方式订阅 需要这个，  queueName必须和这个一致
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverPolicy:  nats.DeliverNewPolicy, // 只接收创建后最新的推送， 创建后重启过程中没有接收的消息在重启后会继续接收
		FilterSubjects: []string{"messages.*", "alerts.>"},
		MaxDeliver:     5,               // 最多尝试次数
		AckWait:        5 * time.Second, // 应答等待时间
	})

	jt.QueueSubscribe(context.Background(), "alerts.>", "sonsumer-queue", "push-messsage", 5, QueueHandler)

	select {}
}

type MyMessage struct {
	ID       int    `json:"id"`
	Content  string `json:"content"`
	Topic    string `json:"topic"`
	SendTime string `json:"send_time"`
}

func PullHandler(msg *nats.Msg) {
	var de = MyMessage{}
	json.Unmarshal(msg.Data, &de)
	log.Println("PullHandler Received:", de)
	if v := msg.Header.Get("Delay-Time"); len(v) > 0 {
		mg, _ := msg.Metadata()
		fmt.Println("投递次数", mg.NumDelivered, mg.Consumer)
		//msg.NakWithDelay(time.Second * 5)
		msg.Ack()
	} else {
		msg.Ack()
	}
	return
}

func QueueHandler(msg *nats.Msg) {
	var de = MyMessage{}
	json.Unmarshal(msg.Data, &de)
	log.Println("QueueHandler Received:", de)
	if v := msg.Header.Get("Delay-Time"); len(v) > 0 {
		mg, _ := msg.Metadata()
		fmt.Println("QueueHandler, 投递次数", mg.NumDelivered, mg.Consumer)
		if mg.NumDelivered > 3 {
			msg.Ack()
		}
		msg.NakWithDelay(time.Second * 5)

	} else {
		msg.Ack()
	}
	return
}

func QueryAllConsumerInfo(stream string) []*nats.ConsumerInfo {
	jy := jt.Js.Consumers(stream)
	var consumer = []*nats.ConsumerInfo{}
	for {
		select {
		case ki := <-jy:
			if ki == nil {
				return consumer
			}
			consumer = append(consumer, ki)
		}
	}
	return consumer
}

// rpc 响应模式
func Response() {
	ctx := context.Background()

	jt.Response(ctx, "request-test", func(msg *nats.Msg) {
		fmt.Println("收到reques消息", string(msg.Data))
		msg.Respond([]byte("哈哈我收到了"))
	})
	select {}

}
