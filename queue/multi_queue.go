package queue

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	redis_helper "github.com/zhangsq-ax/redis-helper-go"
	"github.com/zhangsq-ax/redis-helper-go/distributed_mutex"
	"math"
	"strconv"
	"time"
)

type MultiQueue struct {
	client            *redis.Client
	mutex             *distributed_mutex.DistributedMutex
	descMode          bool
	redisMajorVersion int
}

func NewMultiQueue(client *redis.Client, mutexKey string, mutexTtl time.Duration, descMode ...bool) (*MultiQueue, error) {
	desc := false
	if len(descMode) > 0 {
		desc = descMode[0]
	}
	if mutexKey == "" {
		return nil, fmt.Errorf("mutexKey is empty")
	}
	if mutexTtl <= 0 {
		return nil, fmt.Errorf("mutexTtl is invalid")
	}

	majorVersion, _ := redis_helper.ServerMajorVersion(client)

	return &MultiQueue{
		client:            client,
		mutex:             distributed_mutex.NewDistributedMutex(client, mutexKey, mutexTtl),
		descMode:          desc,
		redisMajorVersion: majorVersion,
	}, nil
}

func (q *MultiQueue) Push(queueKey string, item string, score float64) error {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey, 0)
	defer q.mutex.MustReleaseLockWithSubKey(queueKey, releaseKey)

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

func (q *MultiQueue) Insert(queueKey string, item string, index int64) error {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey, 0)
	defer q.mutex.MustReleaseLockWithSubKey(queueKey, releaseKey)
	if index < 0 {
		return fmt.Errorf("index is invalid")
	}

	score, err := q.GetScoreByIndex(queueKey, index)
	if err != nil {
		return err
	}
	if q.descMode {
		if score != math.MaxFloat64 {
			score += 0.05
		}
	} else {
		if score != -math.MaxFloat64 {
			score -= 0.05
		}
	}

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

func (q *MultiQueue) GetMemberByIndex(queueKey string, index int64) (*redis.Z, error) {
	var (
		members []redis.Z
		err     error
	)
	if q.descMode {
		members, err = q.client.ZRevRangeWithScores(context.Background(), queueKey, index, index).Result()
	} else {
		members, err = q.client.ZRangeWithScores(context.Background(), queueKey, index, index).Result()
	}
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	} else {
		return &members[0], nil
	}
}

func (q *MultiQueue) GetScoreByIndex(queueKey string, index int64) (float64, error) {
	member, err := q.GetMemberByIndex(queueKey, index)
	if err != nil {
		return 0, err
	}
	if member == nil {
		size, err := q.Size(queueKey)
		if err != nil {
			return 0, err
		}
		if size == 0 {
			if q.descMode {
				return math.MaxFloat64, nil
			} else {
				return -math.MaxFloat64, nil
			}
		} else {
			member, err = q.GetMemberByIndex(queueKey, size-1)
			if err != nil {
				return 0, err
			}
		}
	}
	return member.Score, nil
}

func (q *MultiQueue) Pop(queueKey string) (string, float64, error) {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey, 0)
	defer q.mutex.MustReleaseLockWithSubKey(queueKey, releaseKey)

	var (
		members []redis.Z
		err     error
	)
	if q.descMode {
		if q.redisMajorVersion > 4 && q.redisMajorVersion > 0 {
			members, err = q.client.ZPopMax(context.Background(), queueKey).Result()
		} else {
			members, err = zPopMax(q.client, queueKey)
		}
	} else {
		if q.redisMajorVersion > 4 && q.redisMajorVersion > 0 {
			members, err = q.client.ZPopMin(context.Background(), queueKey).Result()
		} else {
			members, err = zPopMin(q.client, queueKey)
		}
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
		return q.client.ZRevRank(context.Background(), queueKey, item).Result()
	} else {
		return q.client.ZRank(context.Background(), queueKey, item).Result()
	}
}

func (q *MultiQueue) Exists(queueKey string, item string) (bool, error) {
	_, err := q.client.ZScore(context.Background(), queueKey, item).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (q *MultiQueue) Each(queueKey string, match string, fn func(member string, score float64) (bool, error)) error {
	var (
		cursor  = uint64(0)
		ctx     = context.Background()
		err     error
		members []string
		ok      bool
	)
	for {
		members, cursor, err = q.client.ZScan(ctx, queueKey, cursor, match, 10).Result()
		if err != nil {
			return err
		}
		for i := 0; i < len(members); i += 2 {
			member := members[i]
			score, _ := strconv.ParseFloat(members[i+1], 64)
			ok, err = fn(member, score)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (q *MultiQueue) Remove(queueKey string, item string) error {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey, 0)
	defer q.mutex.MustReleaseLockWithSubKey(queueKey, releaseKey)

	return q.client.ZRem(context.Background(), queueKey, item).Err()
}

func (q *MultiQueue) Clean(queueKey string, keepFilter func(member redis.Z) bool) error {
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey, 10*time.Second)
	defer q.mutex.MustReleaseLockWithSubKey(queueKey, releaseKey)

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
	releaseKey := q.mutex.MustAcquireLockWithSubKey(queueKey, 0)
	defer q.mutex.MustReleaseLockWithSubKey(queueKey, releaseKey)

	return q.client.Del(context.Background(), queueKey).Err()
}
