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

type clientConsumer interface {
	connection() *kafka.Consumer
	readMessage(t time.Duration) (*kafka.Message, error)
	subscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	commit() ([]kafka.TopicPartition, error)
	close() (err error)
}

type confluentConsumer struct {
	conn *kafka.Consumer
}

func (c *confluentConsumer) connection() *kafka.Consumer {
	return c.conn
}

func (c *confluentConsumer) readMessage(t time.Duration) (*kafka.Message, error) {
	return c.conn.ReadMessage(t)
}

func (c *confluentConsumer) subscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error) {
	return c.conn.SubscribeTopics(topics, rebalanceCb)
}

func (c *confluentConsumer) commit() ([]kafka.TopicPartition, error) {
	return c.conn.Commit()
}

func (c *confluentConsumer) close() (err error) {
	return c.conn.Close()
}
