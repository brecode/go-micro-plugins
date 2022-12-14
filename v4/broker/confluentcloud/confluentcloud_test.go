package confluentcloud

import (
	"context"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-micro/plugins/v4/logger/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go-micro.dev/v4/broker"
	"go-micro.dev/v4/logger"
	"testing"
)

type mockConfluentProducer struct {
	mock.Mock
}

func (p *mockConfluentProducer) produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	args := p.Called(msg, deliveryChan)
	return args.Error(0)
}

func (p *mockConfluentProducer) connection() *kafka.Producer {
	args := p.Called()
	return args.Get(0).(*kafka.Producer)
}

func (p *mockConfluentProducer) close() {
	p.Called()
}

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
	mockConfluentProducer := &mockConfluentProducer{}
	mockConfluentProducer.On("produce", mock.Anything, mock.Anything, mock.Anything).Return(errMock)

	b.cp = mockConfluentProducer
	err := b.Publish(topic, m)

	assert.Equal(t, err, errMock)
}

var errMock = errors.New("mock")
