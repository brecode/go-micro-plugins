package confluentcloud

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go-micro.dev/v4/broker"
)

// A single publication received by a handler.
type publication struct {
	msg   *broker.Message
	topic string
	ack   func()
	err   error
}

func (p *publication) Topic() string            { return p.topic }
func (p *publication) Message() *broker.Message { return p.msg }
func (p *publication) Ack() error               { p.ack(); return nil }
func (p *publication) Error() error             { return p.err }

// consumerSubscriber is a broker.Subscriber for confluent
type consumerSubscriber struct {
	opts     broker.SubscribeOptions
	topic    string
	consumer *kafka.Consumer
}

func (c *consumerSubscriber) Options() broker.SubscribeOptions {
	return c.opts
}

func (c *consumerSubscriber) Topic() string {
	return c.topic
}

func (c *consumerSubscriber) Unsubscribe() error {
	t := c.consumer.Unsubscribe()
	return t
}
