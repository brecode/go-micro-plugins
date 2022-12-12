package confluentcloud

import (
	"context"
	"fmt"
	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"go-micro.dev/v4/broker"
	"go-micro.dev/v4/logger"
	"go-micro.dev/v4/util/cmd"
	"strconv"
	"time"
)

type confluent struct {
	options broker.Options

	cfg *kafka.ConfigMap
	cg  consumerGetter
	cc  clientConsumer
	pg  producerGetter
	cp  clientProducer

	ctx context.Context
	err error
}

func init() {
	cmd.DefaultBrokers["confluentcloud"] = NewBroker
}

// NewBroker creates a new confluent broker.
func NewBroker(opts ...broker.Option) broker.Broker {
	// if no logger set, default to following
	options := broker.Options{
		Logger: logger.DefaultLogger,
	}

	for _, o := range opts {
		o(&options)
	}

	c := new(confluent)
	c.options = options

	if kafkaCfg, ok := options.Context.Value(struct{}{}).(*kafka.ConfigMap); ok {
		c.options.Logger.Log(logger.DebugLevel, "kafka config: %v", kafkaCfg)
		c.cfg = kafkaCfg
	}
	c.cg = func() (*kafka.Consumer, error) {
		return kafka.NewConsumer(c.cfg)
	}
	c.pg = func() (*kafka.Producer, error) {
		return kafka.NewProducer(c.cfg)
	}

	c.ctx = options.Context

	return c
}

func (c *confluent) Init(options ...broker.Option) error {
	c.options.Logger.Log(logger.InfoLevel, "initializing confluent broker")

	for _, o := range options {
		o(&c.options)
	}
	return nil
}

func (c *confluent) Options() broker.Options {
	return c.options
}

func (c *confluent) Address() string {
	addr, _ := c.cfg.Get("bootstrap.servers", "ConfluentCloud")
	return addr.(string)
}

func (c *confluent) Connect() error {
	var err error

	cc, err := NewConsumer(c.cg).getFunc()
	if err != nil {
		return err
	}

	c.cc = &confluentConsumer{
		conn: cc,
	}

	cp, err := NewProducer(c.pg).getFunc()
	if err != nil {
		return err
	}

	c.cp = &confluentProducer{
		conn: cp,
	}

	return nil
}

func (c *confluent) Disconnect() error {
	c.cp.close()
	return c.cc.close()
}

func (c *confluent) Publish(topic string, m *broker.Message, opts ...broker.PublishOption) error {

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

	err := c.cp.produce(msg, nil)
	return err
}

// Subscribe registers a subscription to the given topic against a confluent broker
func (c *confluent) Subscribe(topic string, h broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {

	var options broker.SubscribeOptions
	for _, o := range opts {
		o(&options)
	}

	err := c.cc.subscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				ev, err := c.cc.readMessage(100 * time.Millisecond)
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
					_, err := c.cc.commit()
					logger.Log(logger.ErrorLevel, "commit error: %v for message with key %s", err, string(ev.Key))
				}
			}
		}
	}()

	return &consumerSubscriber{
		opts:     options,
		topic:    topic,
		consumer: c.cc.connection(),
	}, nil
}

func (c *confluent) String() string {
	return "confluentcloud"
}
