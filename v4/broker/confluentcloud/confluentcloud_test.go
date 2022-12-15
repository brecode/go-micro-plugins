package confluentcloud

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-micro/plugins/v4/broker/confluentcloud/mocks"
	"github.com/go-micro/plugins/v4/logger/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go-micro.dev/v4/broker"
	"go-micro.dev/v4/logger"
	"math/rand"
	"testing"
	"time"
)

var errMock = errors.New("mock")

func buildBroker() broker.Broker {
	cfg := &kafka.ConfigMap{}
	ctx := context.Background()

	b := NewBroker(func(options *broker.Options) {
		options.Context = context.WithValue(ctx, struct{}{}, cfg)
	})
	return b
}

func TestNewBroker(t *testing.T) {
	b := buildBroker()

	assert.NotNil(t, b)
}

func TestInitBroker(t *testing.T) {
	b := buildBroker()
	// set some new options i.e., new logger
	tLogger := logrus.NewLogger(func(o *logger.Options) {
		o.Level = logger.DebugLevel
	})

	err := b.Init(func(options *broker.Options) {
		options.Logger = tLogger
	})

	assert.Nil(t, err)
}

func TestAddress(t *testing.T) {
	// test empty address
	b := buildBroker()
	assert.Equal(t, "empty", b.Address())

	sAddress := fmt.Sprintf("%s", "test.example.com:8080")
	cfg := &kafka.ConfigMap{
		"bootstrap.servers": sAddress,
	}
	err := b.Init(func(options *broker.Options) {
		options.Context = context.WithValue(b.Options().Context, struct{}{}, cfg)
	})

	assert.Nil(t, err)
	assert.Equal(t, sAddress, b.Address())
}

func TestConnect(t *testing.T) {
	c := new(confluent)
	c.cfg = &kafka.ConfigMap{}
	c.ctx = context.Background()

	c.cg = func() (*kafka.Consumer, error) {
		return &kafka.Consumer{}, nil
	}

	c.pg = func() (*kafka.Producer, error) {
		return &kafka.Producer{}, nil
	}
	err := c.Connect()
	assert.Nil(t, err)
}

func TestOptions(t *testing.T) {
	b := buildBroker()

	o := b.Options()
	assert.NotNil(t, o)

}

func TestString(t *testing.T) {
	b := buildBroker()

	s := b.String()
	expectedString := "confluentcloud"
	assert.Equal(t, expectedString, s)

}

func TestPublish(t *testing.T) {
	topic := "test-topic"
	m := &broker.Message{
		Header: map[string]string{
			"test1": "test1",
			"test2": "test2",
		},
		Body: []byte{},
	}

	b := new(confluent)
	mockConfluentProducer := mocks.NewClientProducer(t)
	mockConfluentProducer.On("Produce", mock.Anything, mock.Anything, mock.Anything).Return(errMock)
	b.cp = mockConfluentProducer
	err := b.Publish(topic, m)

	assert.Equal(t, err, errMock)
}

func TestSubscribe(t *testing.T) {
	topic := "test-topic"
	body := make([]byte, 10)
	rand.Read(body)
	m := &kafka.Message{
		Headers: []kafka.Header{
			{
				Key:   "partition",
				Value: []byte{0},
			},
		},
		Value: body,
		TopicPartition: kafka.TopicPartition{
			Partition: 0,
			Topic:     &topic,
		},
	}

	b := new(confluent)
	b.ctx = context.Background()
	h := func(event broker.Event) error {
		return nil
	}
	b.isTesting = true

	mockClientConsumer := mocks.NewClientConsumer(t)
	b.cc = mockClientConsumer

	mockClientConsumer.On("SubscribeTopics", []string{topic}, mock.Anything).Return(nil)
	mockClientConsumer.On("Connection").Return(&kafka.Consumer{}, nil)
	mockClientConsumer.On("ReadMessage", time.Millisecond*100).Return(m, nil)
	mockClientConsumer.On("Commit").Return(nil, nil)

	_, err := b.Subscribe(topic, h)
	assert.Equal(t, err, nil)
}
