package event_bus

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zhangsq-ax/redis-helper-go/message_queue"
)

type EventBus struct {
	mqClient           *message_queue.MessageQueueClient
	eventChannelPrefix string
}

func NewEventBus(client *redis.Client, eventChannelPrefix string) *EventBus {
	return &EventBus{
		mqClient:           message_queue.NewMessageQueueClient(client),
		eventChannelPrefix: eventChannelPrefix,
	}
}

func (eb *EventBus) FullChannel(eventName string) string {
	return fmt.Sprintf("%s%s", eb.eventChannelPrefix, eventName)
}

func (eb *EventBus) Trigger(eventName string, payload []byte) error {
	return eb.mqClient.Publish(eb.FullChannel(eventName), payload)
}

func (eb *EventBus) AddEventListener(eventName string, handler func(eventName string, payload []byte)) (string, error) {
	return eb.mqClient.Subscribe(context.Background(), func(msg *redis.Message) {
		handler(eventName, []byte(msg.Payload))
	}, eb.FullChannel(eventName))
}

func (eb *EventBus) RemoveEventListener(subscribeId string) {
	eb.mqClient.UnSubscribe(subscribeId)
}

func (eb *EventBus) On(eventName string, handler func(eventName string, payload []byte)) (string, error) {
	return eb.AddEventListener(eventName, handler)
}

func (eb *EventBus) Once(eventName string, handler func(eventName string, payload []byte)) error {
	var (
		subscribeId string
		err         error
	)
	subscribeId, err = eb.On(eventName, func(eventName string, payload []byte) {
		if handler != nil {
			go handler(eventName, payload)
			eb.RemoveEventListener(subscribeId)
			handler = nil
		}
	})
	return err
}
