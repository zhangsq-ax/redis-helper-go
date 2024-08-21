package queue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zhangsq-ax/redis-helper-go/distributed_mutex"
	"time"
)

type MultiQueue struct {
	client   *redis.Client
	mutex    *distributed_mutex.DistributedMutex
	descMode bool
}

func NewMultiQueue(client *redis.Client, descMode ...bool) *MultiQueue {
	desc := false
	if len(descMode) > 0 {
		desc = descMode[0]
	}
	return &MultiQueue{
		client:   client,
		mutex:    distributed_mutex.NewDistributedMutex(client, "mutex", 5*time.Second),
		descMode: desc,
	}
}

func (q *MultiQueue) Push(queueKey string, item string, score float64) error {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey)
	defer q.mutex.ReleaseLockWithSubKey(queueKey, releaseKey)

	count, err := q.client.ZAdd(context.Background(), queueKey, redis.Z{
		Score:  score,
		Member: item,
	}).Result()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("exist in queue")
	}
	return nil
}

func (q *MultiQueue) Pop(queueKey string) (string, float64, error) {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey)
	defer q.mutex.ReleaseLockWithSubKey(queueKey, releaseKey)

	var (
		members []redis.Z
		err     error
	)
	if q.descMode {
		members, err = q.client.ZPopMax(context.Background(), queueKey).Result()
	} else {
		members, err = q.client.ZPopMin(context.Background(), queueKey).Result()
	}
	if err != nil {
		return "", 0, err
	}
	if len(members) == 0 {
		return "", 0, fmt.Errorf("no member in queue")
	}
	return members[0].Member.(string), members[0].Score, nil
}

func (q *MultiQueue) Size(queueKey string) (int64, error) {
	return q.client.ZCard(context.Background(), queueKey).Result()
}

func (q *MultiQueue) Rank(queueKey string, item string) (int64, error) {
	if q.descMode {
		return q.client.ZRank(context.Background(), queueKey, item).Result()
	} else {
		return q.client.ZRevRank(context.Background(), queueKey, item).Result()
	}
}

func (q *MultiQueue) Remove(queueKey string, item string) error {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey)
	defer q.mutex.ReleaseLockWithSubKey(queueKey, releaseKey)

	return q.client.ZRem(context.Background(), queueKey, item).Err()
}

func (q *MultiQueue) Clean(queueKey string, keepFilter func(member redis.Z) bool) error {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey, 10*time.Second)
	defer q.mutex.ReleaseLockWithSubKey(queueKey, releaseKey)

	members, err := q.client.ZRangeWithScores(context.Background(), queueKey, 0, -1).Result()
	if err != nil {
		return err
	}

	needRemoveItems := make([]any, 0)
	for _, member := range members {
		if !keepFilter(member) {
			needRemoveItems = append(needRemoveItems, member.Member)
		}
	}

	return q.client.ZRem(context.Background(), queueKey, needRemoveItems...).Err()
}

func (q *MultiQueue) Clear(queueKey string) error {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey)
	defer q.mutex.ReleaseLockWithSubKey(queueKey, releaseKey)

	return q.client.Del(context.Background(), queueKey).Err()
}
