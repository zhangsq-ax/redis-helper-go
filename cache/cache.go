package cache

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type Cache struct {
	client         *redis.Client
	cacheKeyPrefix string
}

func NewCache(client *redis.Client, cacheKeyPrefix string) *Cache {
	return &Cache{
		client:         client,
		cacheKeyPrefix: cacheKeyPrefix,
	}
}

func (c *Cache) cacheKey(key string) string {
	return fmt.Sprintf("%s%s", c.cacheKeyPrefix, key)
}

func (c *Cache) Cache(key string, value string, expiration time.Duration) error {
	return c.client.SetEx(context.Background(), c.cacheKey(key), value, expiration).Err()
}

func (c *Cache) Get(key string) (string, error) {
	return c.client.Get(context.Background(), c.cacheKey(key)).Result()
}

func (c *Cache) Exists(key string) (bool, error) {
	count, err := c.client.Exists(context.Background(), c.cacheKey(key)).Result()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (c *Cache) Delete(key string) error {
	return c.client.Del(context.Background(), c.cacheKey(key)).Err()
}
