package confluentcloud

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go-micro.dev/v4/broker"
	"go-micro.dev/v4/util/cmd"
	"strconv"
	"time"
)

type Confluent struct {
	options broker.Options

	cfg      *kafka.ConfigMap
	consumer *kafka.Consumer
	producer *kafka.Producer
	err      error
}

func init() {
	cmd.DefaultBrokers["confluentcloud"] = NewBroker
}

// NewBroker creates a new confluent broker.
func NewBroker(opts ...broker.Option) broker.Broker {
	options := &broker.Options{}

	for _, o := range opts {
		o(options)
	}

	c := new(Confluent)
	if kafkaCfg, ok := options.Context.Value(struct{}{}).(*kafka.ConfigMap); ok {
		fmt.Printf("%v", kafkaCfg)
		c.cfg = kafkaCfg
	}

	err := c.setupConfluent()
	if err != nil {
		c.err = err
	}
	return c
}
func (c *Confluent) Init(option ...broker.Option) error {
	return errors.New("unimplemented; pass options to NewBroker instead")
}

func (c *Confluent) setupConfluent() error {
	var err error

	c.consumer, err = kafka.NewConsumer(c.cfg)
	if err != nil {
		fmt.Println(err)
		return err
	}

	c.producer, err = kafka.NewProducer(c.cfg)
	if err != nil {
		return err
	}

	return nil
}

func (c *Confluent) Options() broker.Options {
	return c.options
}

func (c *Confluent) Address() string {
	addr, _ := c.cfg.Get("bootstrap.servers", "ConfluentCloud")
	return addr.(string)
}

func (c *Confluent) Connect() error {
	return nil
}

func (c *Confluent) Disconnect() error {
	c.consumer.Close()
	c.producer.Close()
	return nil
}

func (c *Confluent) Publish(topic string, m *broker.Message, opts ...broker.PublishOption) error {

	var options broker.PublishOptions
	for _, o := range opts {
		o(&options)
	}

	p, _ := strconv.ParseInt(m.Header["partition"], 10, 32)
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(p),
		},
		Value: m.Body,
	}

	err := c.producer.Produce(msg, nil)
	return err
}

func (c *Confluent) Subscribe(topic string, h broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {

	var options broker.SubscribeOptions
	for _, o := range opts {
		o(&options)
	}

	err := c.consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()

	go func() {
		for {
			select {
			case <-ctx.Done():
				c.consumer.Close()
				return
			default:
				ev, err := c.consumer.ReadMessage(100 * time.Millisecond)
				if err != nil {
					continue
				}

				header := make(map[string]string)
				for _, v := range ev.Headers {
					header[v.Key] = fmt.Sprintf("%v", v.Value)
				}

				// add partition to header
				header["partition"] = fmt.Sprint(ev.TopicPartition.Partition)
				p := &publication{
					msg: &broker.Message{
						Header: header,
						Body:   ev.Value,
					},
					topic: *ev.TopicPartition.Topic,
				}
				if err := h(p); err != nil {
					fmt.Println(err)
					continue
				}
			}
		}
	}()

	return &consumer{
		opts:     options,
		topic:    topic,
		consumer: c.consumer,
	}, nil
}

func (c *Confluent) String() string {
	return "confluentcloud"
}

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
