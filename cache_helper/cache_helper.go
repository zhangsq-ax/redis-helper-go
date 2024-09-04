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

func (c *CacheHelper) FullCacheKey(key string) string {
	return fmt.Sprintf("%s%s", c.cacheKeyPrefix, key)
}

func (c *CacheHelper) Cache(key string, value any, expiration time.Duration) (string, error) {
	cacheKey := c.FullCacheKey(key)
	if expiration == 0 {
		return cacheKey, c.client.Set(context.Background(), cacheKey, value, 0).Err()
	}
	return cacheKey, c.client.SetEx(context.Background(), cacheKey, value, expiration).Err()
}

func (c *CacheHelper) Get(key string) (string, error) {
	return c.client.Get(context.Background(), c.FullCacheKey(key)).Result()
}

func (c *CacheHelper) Exists(key string) (bool, error) {
	count, err := c.client.Exists(context.Background(), c.FullCacheKey(key)).Result()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (c *CacheHelper) ResetExpiration(key string, expiration time.Duration) error {
	return c.client.Expire(context.Background(), c.FullCacheKey(key), expiration).Err()
}

func (c *CacheHelper) Each(keyPattern string, fn func(key string) (bool, error)) error {
	var (
		cursor = uint64(0)
		keys   []string
		err    error
	)
	for {
		keys, cursor, err = c.client.Scan(context.Background(), cursor, keyPattern, 10).Result()
		if err != nil {
			return err
		}
		if cursor == 0 {
			break
		}
		for _, key := range keys {
			ok, err := fn(key)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
	}
	return nil
}

func (c *CacheHelper) EachWithValue(keyPattern string, fn func(key string, value string) (bool, error)) error {
	return c.Each(keyPattern, func(key string) (bool, error) {
		value, err := c.client.Get(context.Background(), key).Result()
		if err != nil {
			return false, err
		}
		return fn(key, value)
	})
}

func (c *CacheHelper) Delete(key string) error {
	return c.client.Del(context.Background(), c.FullCacheKey(key)).Err()
}
