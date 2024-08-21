package key_monitor

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zhangsq-ax/redis-helper-go/message_queue"
)

type KeyMonitor struct {
	mqClient *message_queue.MessageQueueClient
}

func NewKeyMonitor(client *redis.Client) *KeyMonitor {
	return &KeyMonitor{
		mqClient: message_queue.NewMessageQueueClient(client),
	}
}

func (km *KeyMonitor) WatchKeyEvent(ctx context.Context, eventName string, filter func(msg *redis.Message) bool, handler func(msg *redis.Message)) error {
	channel := fmt.Sprintf("__keyevent@%d__:%s", km.mqClient.Options().DB, eventName)
	_, err := km.mqClient.Subscribe(ctx, func(msg *redis.Message) {
		if filter != nil && !filter(msg) {
			return
		}
		handler(msg)
	}, channel)
	return err
}

func (km *KeyMonitor) WatchKeyExpiredEvent(ctx context.Context, filter func(msg *redis.Message) bool, handler func(msg *redis.Message)) error {
	return km.WatchKeyEvent(ctx, "expired", filter, handler)
}

func (km *KeyMonitor) WatchKeySpace(ctx context.Context, key string, filter func(msg *redis.Message) bool, handler func(msg *redis.Message)) error {
	channel := fmt.Sprintf("__keyspace@%d__:%s", km.mqClient.Options().DB, key)
	_, err := km.mqClient.Subscribe(ctx, func(msg *redis.Message) {
		if filter != nil && !filter(msg) {
			return
		}
		handler(msg)
	}, channel)
	return err
}
