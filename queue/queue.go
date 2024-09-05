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

type Queue struct {
	client            *redis.Client
	mutex             *distributed_mutex.DistributedMutex
	key               string
	descMode          bool
	redisMajorVersion int
}

func NewQueue(client *redis.Client, key string, descMode ...bool) *Queue {
	desc := false
	if len(descMode) > 0 {
		desc = descMode[0]
	}

	majorVersion, _ := redis_helper.ServerMajorVersion(client)

	return &Queue{
		client:            client,
		mutex:             distributed_mutex.NewDistributedMutex(client, fmt.Sprintf("%s:mutex", key), 5*time.Second),
		key:               key,
		descMode:          desc,
		redisMajorVersion: majorVersion,
	}
}

func (q *Queue) Push(item string, score float64) error {
	releaseKey := q.mutex.MustAcquireLock(0)
	defer q.mutex.MustReleaseLock(releaseKey)

	count, err := q.client.ZAdd(context.Background(), q.key, redis.Z{
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

func (q *Queue) Insert(item string, index int64) error {
	releaseKey := q.mutex.MustAcquireLock(0)
	defer q.mutex.MustReleaseLock(releaseKey)
	if index < 0 {
		return fmt.Errorf("index is invalid")
	}

	score, err := q.GetScoreByIndex(index)
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

	count, err := q.client.ZAdd(context.Background(), q.key, redis.Z{
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

func (q *Queue) GetByIndex(index int64) (string, error) {
	member, err := q.GetMemberByIndex(index)
	if err != nil {
		return "", err
	}
	if member == nil {
		return "", nil
	}
	return member.Member.(string), nil
}

func (q *Queue) GetMemberByIndex(index int64) (*redis.Z, error) {
	var (
		members []redis.Z
		err     error
	)
	if q.descMode {
		members, err = q.client.ZRevRangeWithScores(context.Background(), q.key, index, index).Result()
	} else {
		members, err = q.client.ZRangeWithScores(context.Background(), q.key, index, index).Result()
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

func (q *Queue) GetScoreByIndex(index int64) (float64, error) {
	member, err := q.GetMemberByIndex(index)
	if err != nil {
		return 0, err
	}
	if member == nil {
		size, err := q.Size()
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
			member, err = q.GetMemberByIndex(size - 1)
			if err != nil {
				return 0, err
			}
		}
	}
	return member.Score, nil
}

func (q *Queue) Pop() (string, float64, error) {
	releaseKey := q.mutex.MustAcquireLock(0)
	defer q.mutex.MustReleaseLock(releaseKey)

	var (
		members []redis.Z
		err     error
	)
	if q.descMode {
		if q.redisMajorVersion > 4 && q.redisMajorVersion > 0 {
			members, err = q.client.ZPopMax(context.Background(), q.key).Result()
		} else {
			members, err = zPopMax(q.client, q.key)
		}
	} else {
		if q.redisMajorVersion > 4 && q.redisMajorVersion > 0 {
			members, err = q.client.ZPopMin(context.Background(), q.key).Result()
		} else {
			members, err = zPopMin(q.client, q.key)
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

func (q *Queue) Size() (int64, error) {
	return q.client.ZCard(context.Background(), q.key).Result()
}

func (q *Queue) Rank(item string) (int64, error) {
	if q.descMode {
		return q.client.ZRevRank(context.Background(), q.key, item).Result()
	} else {
		return q.client.ZRank(context.Background(), q.key, item).Result()
	}
}

func (q *Queue) Exists(item string) (bool, error) {
	_, err := q.client.ZScore(context.Background(), q.key, item).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (q *Queue) Each(match string, fn func(member string, score float64) (bool, error)) error {
	var (
		cursor  = uint64(0)
		ctx     = context.Background()
		err     error
		members []string
		ok      bool
	)
	for {
		members, cursor, err = q.client.ZScan(ctx, q.key, cursor, match, 10).Result()
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

func (q *Queue) Remove(item string) error {
	releaseKey := q.mutex.MustAcquireLock(0)
	defer q.mutex.MustReleaseLock(releaseKey)

	return q.client.ZRem(context.Background(), q.key, item).Err()
}

func (q *Queue) Clean(keepFilter func(member redis.Z) bool) error {
	releaseKey := q.mutex.MustAcquireLock(10 * time.Second)
	defer q.mutex.MustReleaseLock(releaseKey)

	members, err := q.client.ZRangeWithScores(context.Background(), q.key, 0, -1).Result()
	if err != nil {
		return err
	}

	needRemoveItems := make([]any, 0)
	for _, member := range members {
		if !keepFilter(member) {
			needRemoveItems = append(needRemoveItems, member.Member)
		}
	}

	return q.client.ZRem(context.Background(), q.key, needRemoveItems...).Err()
}

func (q *Queue) Clear() error {
	releaseKey := q.mutex.MustAcquireLock(0)
	defer q.mutex.MustReleaseLock(releaseKey)

	return q.client.Del(context.Background(), q.key).Err()
}

func zPopMin(client *redis.Client, key string) ([]redis.Z, error) {
	luaScript := `
local min_element = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #min_element > 0 then
    redis.call('ZREM', KEYS[1], min_element[1])
end
return min_element`

	res, err := client.Eval(context.Background(), luaScript, []string{key}).Result()
	if err != nil {
		return nil, err
	}
	record := res.([]any)
	result := make([]redis.Z, 0)
	if len(record) > 0 {
		member := record[0].(string)
		score, err := strconv.ParseFloat(record[1].(string), 64)
		if err != nil {
			return nil, err
		}
		result = append(result, redis.Z{Member: member, Score: score})
	}

	return result, nil
}

func zPopMax(client *redis.Client, key string) ([]redis.Z, error) {
	luaScript := `
local max_element = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #max_element > 0 then
    redis.call('ZREM', KEYS[1], max_element[1])
end
return max_element`

	res, err := client.Eval(context.Background(), luaScript, []string{key}).Result()
	if err != nil {
		return nil, err
	}
	record := res.([]any)
	result := make([]redis.Z, 0)
	if len(record) > 0 {
		member := record[0].(string)
		score, err := strconv.ParseFloat(record[1].(string), 64)
		if err != nil {
			return nil, err
		}
		result = append(result, redis.Z{Member: member, Score: score})
	}

	return result, nil
}
