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

type clientProducer interface {
	produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	connection() *kafka.Producer
	close()
}

func (p *confluentProducer) connection() *kafka.Producer {
	return p.conn
}

func (p *confluentProducer) produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return p.conn.Produce(msg, deliveryChan)
}

func (p *confluentProducer) close() {
	p.conn.Close()
}
