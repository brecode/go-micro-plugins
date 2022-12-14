package confluentcloud

import "github.com/confluentinc/confluent-kafka-go/kafka"

type producerGetter func() (*kafka.Producer, error)

type producer struct {
	getFunc producerGetter
}

func NewProducer(cg producerGetter) *producer {
	return &producer{getFunc: cg}
}

type confluentProducer struct {
	conn *kafka.Producer
}

type ClientProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Connection() *kafka.Producer
	Close()
}

func (p *confluentProducer) Connection() *kafka.Producer {
	return p.conn
}

func (p *confluentProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return p.conn.Produce(msg, deliveryChan)
}

func (p *confluentProducer) Close() {
	p.conn.Close()
}
