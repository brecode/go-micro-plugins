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

// consumer is a broker.Subscriber for confluent
type consumer struct {
	opts     broker.SubscribeOptions
	topic    string
	consumer *kafka.Consumer
}

func (m *consumer) Options() broker.SubscribeOptions {
	return m.opts
}

func (m *consumer) Topic() string {
	return m.topic
}

func (m *consumer) Unsubscribe() error {
	t := m.consumer.Unsubscribe()
	return t
}
