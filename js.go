package bnats

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	"log/slog"
	"time"
)

type JetStreamConsumer struct {
	Connect    *nats.Conn
	Js         nats.JetStreamContext
	StreamName string // 必须要调用  AddStream 后才会有值
	cancel     context.CancelFunc
	ctx        context.Context
}

func NewJetStreamConsumer(natsUrl string) *JetStreamConsumer {
	// 连接 NATS
	nc, err := nats.Connect(natsUrl)
	if err != nil {
		slog.Error("链接nats server失败", "error", err.Error())
		return nil
	}
	Js, _ := nc.JetStream()
	ctx, cancel := context.WithCancel(context.Background())
	return &JetStreamConsumer{
		Connect:    nc,
		Js:         Js,
		StreamName: "default",
		cancel:     cancel,
		ctx:        ctx,
	}
}

func (j *JetStreamConsumer) AddStream(sc *nats.StreamConfig) error {
	j.StreamName = sc.Name
	_, err := j.Js.StreamInfo(sc.Name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		// 创建 Stream（如果不存在）
		_, err = j.Js.AddStream(sc)
		if err != nil {
			slog.Error("添加stream", "error", err.Error())
		}
		return err
	} else {
		_, err := j.Js.UpdateStream(sc)
		if err != nil {
			slog.Error("更新stream", "error", err.Error())
			return err
		}
	}
	return nil

}

// 创建消费者配置， 每个消费者都要创建一个
func (j *JetStreamConsumer) AddConsumer(ctx context.Context, stream string, cfg *nats.ConsumerConfig, opts ...nats.JSOpt) {
	_, err := j.Js.ConsumerInfo(stream, cfg.Durable, opts...)
	if errors.Is(err, nats.ErrConsumerNotFound) {
		// 设置一个消费者的配置， 其他的消费函数可以共用
		_, err = j.Js.AddConsumer(stream, cfg, opts...)
		if err != nil {
			slog.ErrorContext(ctx, "AddConsumer", "err", err.Error())
			return
		}
	} else {
		_, err = j.Js.UpdateConsumer(stream, cfg, opts...)
		if err != nil {
			slog.ErrorContext(ctx, "UpdateConsumer", "err", err.Error())
			return
		}
	}
}

func (j *JetStreamConsumer) DeleteConsumer(ctx context.Context, stream string, consumer string) {
	j.Js.DeleteConsumer(stream, consumer)
}

func (j *JetStreamConsumer) Stop() {
	err := j.Connect.Drain()
	if err != nil {
		slog.Error("close jetstream Connection", "name", j.StreamName, "error", err.Error())
		return
	}
}

// 拉取信息
func (j *JetStreamConsumer) PullSubscribe(ctx context.Context, sub, consumerName string, maxConcurrency int, f nats.MsgHandler) {
	subscribe, err := j.Js.PullSubscribe(sub, consumerName)
	if err != nil {
		slog.ErrorContext(ctx, "PullSubscribe", "sub", sub, "consumerName", consumerName, "err", err.Error())
		return
	}
	go func(ctx context.Context, sub *nats.Subscription) {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "PullSubscribe", "err", r)
			}
		}()
		// 并发池（最多 MaxConcurrency 个 worker）
		sem := make(chan struct{}, maxConcurrency)
		// 实际的并发控制量是    maxConcurrency * 5
		for {
			// 拉取最多 5 条消息

			msgs, err := sub.Fetch(5, nats.MaxWait(2*time.Second))
			if err != nil {
				if err == nats.ErrTimeout {
					// 5秒内没有拉到消息就会超时， 所以这里认为是正常的
				} else {
					slog.ErrorContext(ctx, "PullSubscribe", "err", err.Error())
				}
			}
			select {
			case <-ctx.Done():
				slog.InfoContext(ctx, "PullSubscribe Stop")
				return

			default:
				for _, msg := range msgs {
					sem <- struct{}{}
					go func() {
						defer func() {
							if r := recover(); r != nil {
								slog.ErrorContext(ctx, "PullSubscribe Recovered from panic", "err", r)
								msg.Nak()
							}
							<-sem
						}()
						f(msg)
					}()
				}
			}
		}
	}(j.ctx, subscribe)
}

// 服务端推送模式
// queueName 必须和 消费配置的 DeliverGroup 一致
func (j *JetStreamConsumer) QueueSubscribe(ctx context.Context, sub, queueName string, consumerName string, maxConcurrency int, f nats.MsgHandler) {
	for range maxConcurrency {
		subscribe, err := j.Js.QueueSubscribe(sub, queueName, f, nats.Bind(j.StreamName, consumerName))
		if err != nil {
			slog.Error("queue Subscribe", "sub", sub, "consumerName", queueName, "err", err.Error())
			return
		}
		subscribe.SetPendingLimits(-1, -1) // optional: 不限缓冲数量
	}
}

func (j *JetStreamConsumer) Subscribe(ctx context.Context, sub, consumerName string, f nats.MsgHandler) {
	_, err := j.Js.Subscribe(sub, f, nats.Bind(j.StreamName, consumerName))
	if err != nil {
		slog.Error("Subscribe", "sub", sub, "consumerName", consumerName, "err", err.Error())
		return
	}
}

func (j *JetStreamConsumer) PublishMsg(ctx context.Context, msg *nats.Msg) *nats.PubAck {
	publishMsg, err := j.Js.PublishMsg(msg)
	if err != nil {
		slog.Error("PublishMsg", "msg", msg, "err", err.Error())
		return nil
	}
	return publishMsg
}

// 发布并等待返回（Request-Reply 模式）
// f 中 最后需要 msg.Respond([]byte("data")), 返回响应数据
func (j *JetStreamConsumer) Response(ctx context.Context, sub string, f nats.MsgHandler) {
	_, err := j.Connect.Subscribe(sub, f)
	if err != nil {
		slog.Error("Response", "sub", sub, "consumerName", sub, "err", err.Error())
		return
	}
}

// 发布并等待返回 （Request-Reply 模式）
func (j *JetStreamConsumer) Request(ctx context.Context, sub string, data []byte, timeout time.Duration, result any) ([]byte, error) {
	resp, err := j.Connect.Request(sub, data, timeout)
	if err != nil {
		slog.Error("Request", "sub", sub, "consumerName", sub, "err", err.Error())
		return nil, err
	}
	if resp != nil && result != nil {
		err = json.Unmarshal(resp.Data, result)
		if err != nil {
			slog.Error("Request response data Json.Unmarshal", "sub", sub, "consumerName", sub, "err", err.Error())
			return resp.Data, err
		}
	}
	return resp.Data, nil
}
