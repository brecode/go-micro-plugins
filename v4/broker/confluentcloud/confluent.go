package confluentcloud

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go-micro.dev/v4/broker"
	"go-micro.dev/v4/logger"
	"go-micro.dev/v4/util/cmd"
	"strconv"
	"time"
)

type Confluent struct {
	options broker.Options

	cfg      *kafka.ConfigMap
	consumer *kafka.Consumer
	producer *kafka.Producer

	err error
}

func init() {
	cmd.DefaultBrokers["confluentcloud"] = NewBroker
}

// NewBroker creates a new confluent broker.
func NewBroker(opts ...broker.Option) broker.Broker {
	options := &broker.Options{
		Logger: logger.DefaultLogger,
	}

	for _, o := range opts {
		o(options)
	}

	c := new(Confluent)
	if kafkaCfg, ok := options.Context.Value(struct{}{}).(*kafka.ConfigMap); ok {
		logger.Log(logger.DebugLevel, "kafka config: %v", kafkaCfg)
		c.cfg = kafkaCfg
	}
	return c
}

func (c *Confluent) Init(options ...broker.Option) error {
	for _, o := range options {
		o(&c.options)
	}

	// reconfigure for new options, if any
	return c.setupConfluent()
}

func (c *Confluent) setupConfluent() error {
	var err error

	c.consumer, err = kafka.NewConsumer(c.cfg)
	if err != nil {
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
	err := c.consumer.Close()
	c.producer.Close()

	return err
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

	//	context should be coming from outside
	ctx := context.Background()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				ev, err := c.consumer.ReadMessage(100 * time.Millisecond)
				if err != nil {
					logger.Log(logger.ErrorLevel, err)
					continue
				}

				// message processing here ...
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

				err = h(p)
				if err != nil {
					// 1. retry with exponential backoff
					// 2. Max retries
				} else {
					_, err := c.consumer.Commit()
					logger.Log(logger.ErrorLevel, "commit error: %v for message with key %s", err, string(ev.Key))
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
