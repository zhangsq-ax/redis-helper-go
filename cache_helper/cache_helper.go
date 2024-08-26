package cache_helper

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type CacheHelper struct {
	client         *redis.Client
	cacheKeyPrefix string
}

func NewCacheHelper(client *redis.Client, cacheKeyPrefix string) *CacheHelper {
	return &CacheHelper{
		client:         client,
		cacheKeyPrefix: cacheKeyPrefix,
	}
}

func (c *CacheHelper) cacheKey(key string) string {
	return fmt.Sprintf("%s%s", c.cacheKeyPrefix, key)
}

func (c *CacheHelper) Cache(key string, value any, expiration time.Duration) (string, error) {
	cacheKey := c.cacheKey(key)
	if expiration == 0 {
		return cacheKey, c.client.Set(context.Background(), cacheKey, value, 0).Err()
	}
	return cacheKey, c.client.SetEx(context.Background(), cacheKey, value, expiration).Err()
}

func (c *CacheHelper) Get(key string) (string, error) {
	return c.client.Get(context.Background(), c.cacheKey(key)).Result()
}

func (c *CacheHelper) Exists(key string) (bool, error) {
	count, err := c.client.Exists(context.Background(), c.cacheKey(key)).Result()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (c *CacheHelper) Delete(key string) error {
	return c.client.Del(context.Background(), c.cacheKey(key)).Err()
}
