package confluentcloud

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go-micro.dev/v4/broker"
)

type BrokerCfg struct {
	ConfigMap *kafka.ConfigMap
}

func UseBrokerConfig(b *BrokerCfg) broker.Option {
	return optionsFunc(struct{}{}, b)
}

func optionsFunc(key, val interface{}) func(*broker.Options) {
	return func(o *broker.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, key, val)
	}
}
