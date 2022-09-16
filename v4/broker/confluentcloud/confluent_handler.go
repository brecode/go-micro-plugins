package confluentcloud

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go-micro.dev/v4/broker"
)

// pub is a broker.Event
type pub struct {
	topic string
	msg   *broker.Message
	err   error
}

// consumer is a broker.Subscriber
type consumer struct {
	opts     broker.SubscribeOptions
	topic    string
	consumer *kafka.Consumer
}

func (m *pub) Ack() error {
	return nil
}

func (m *pub) Error() error {
	return m.err
}

func (m *pub) Topic() string {
	return m.topic
}

func (m *pub) Message() *broker.Message {
	return m.msg
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
