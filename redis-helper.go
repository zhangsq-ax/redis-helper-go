package redis_helper

import (
	"context"
	"github.com/redis/go-redis/v9"
)

func GetRedisClient(opts *redis.Options) (*redis.Client, error) {
	client := redis.NewClient(opts)
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	return client, nil
}
