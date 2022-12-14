package confluentcloud

import (
	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type consumerGetter func() (*kafka.Consumer, error)

type consumer struct {
	getFunc consumerGetter
}

func NewConsumer(cg consumerGetter) *consumer {
	return &consumer{getFunc: cg}
}

type ClientConsumer interface {
	Connection() *kafka.Consumer
	ReadMessage(t time.Duration) (*kafka.Message, error)
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	Commit() ([]kafka.TopicPartition, error)
	Close() (err error)
}

type confluentConsumer struct {
	conn *kafka.Consumer
}

func (c *confluentConsumer) Connection() *kafka.Consumer {
	return c.conn
}

func (c *confluentConsumer) ReadMessage(t time.Duration) (*kafka.Message, error) {
	return c.conn.ReadMessage(t)
}

func (c *confluentConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error) {
	return c.conn.SubscribeTopics(topics, rebalanceCb)
}

func (c *confluentConsumer) Commit() ([]kafka.TopicPartition, error) {
	return c.conn.Commit()
}

func (c *confluentConsumer) Close() (err error) {
	return c.conn.Close()
}
