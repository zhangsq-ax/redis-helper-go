package message_queue

import (
	"context"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"math"
	"sync"
	"time"
)

type MessageQueueClient struct {
	client      *redis.Client
	subscribers sync.Map
}

func NewMessageQueueClient(client *redis.Client) *MessageQueueClient {
	return &MessageQueueClient{
		client:      client,
		subscribers: sync.Map{},
	}
}

func (mq *MessageQueueClient) Publish(channel string, message any) error {
	return mq.client.Publish(context.Background(), channel, message).Err()
}

func (mq *MessageQueueClient) Subscribe(ctx context.Context, onMessage func(*redis.Message), channels ...string) (string, error) {
	sub := mq.client.PSubscribe(ctx, channels...)
	subscribeId := uuid.New().String()
	mq.subscribers.Store(subscribeId, sub)
	go func() {
		for {
			err := receiveMessageWithRetry(ctx, sub, 5, onMessage)
			if err != nil {
				mq.UnSubscribe(subscribeId)
				break
			}
		}
	}()
	return subscribeId, nil
}

func (mq *MessageQueueClient) UnSubscribe(subscribeId string) {
	s, ok := mq.subscribers.Load(subscribeId)
	if !ok {
		return
	}
	sub, ok := s.(*redis.PubSub)
	if ok {
		_ = sub.Close()
	}
	mq.subscribers.Delete(subscribeId)
}

func receiveMessageWithRetry(ctx context.Context, pubsub *redis.PubSub, maxRetries int, onMessage func(*redis.Message)) error {
	retryCount := 0
	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if retryCount < maxRetries {
				retryCount++
				sleepDuration := time.Duration(math.Pow(2, float64(retryCount))) * time.Second
				time.Sleep(sleepDuration)
				continue
			}
			return err
		}
		onMessage(msg)
		break
	}
	return nil
}
