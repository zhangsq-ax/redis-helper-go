package distributed_mutex

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"time"
)

// DistributedMutex distributed mutex based on redis
type DistributedMutex struct {
	client *redis.Client
	key    string
	ttl    time.Duration
}

// NewDistributedMutex create new distributed mutex
func NewDistributedMutex(client *redis.Client, key string, ttl ...time.Duration) *DistributedMutex {
	keyTtl := 30 * time.Second
	if len(ttl) > 0 {
		keyTtl = ttl[0]
	}
	return &DistributedMutex{
		client: client,
		key:    key,
		ttl:    keyTtl,
	}
}

func (dm *DistributedMutex) getKey(subKey string) string {
	if subKey == "" {
		return dm.key
	}
	return fmt.Sprintf("%s:%s", dm.key, subKey)
}

// AcquireLock try to acquire lock
func (dm *DistributedMutex) AcquireLock(ttl ...time.Duration) (string, bool) {
	return dm.AcquireLockWithSubKey("", ttl...)
}

func (dm *DistributedMutex) AcquireLockWithSubKey(subKey string, ttl ...time.Duration) (string, bool) {
	keyTtl := dm.ttl
	if len(ttl) > 0 {
		keyTtl = ttl[0]
	}
	releaseKey := uuid.New().String()
	success, err := dm.client.SetNX(context.Background(), dm.getKey(subKey), releaseKey, keyTtl).Result()
	if err != nil {
		return "", false
	}
	if success {
		return releaseKey, true
	}
	return "", false
}

func (dm *DistributedMutex) MustAcquireLock(interval time.Duration, ttl ...time.Duration) string {
	var (
		releaseKey string
		success    = false
	)
	if interval < 50*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	for {
		releaseKey, success = dm.AcquireLock(ttl...)
		if success {
			break
		}
		time.Sleep(interval)
	}
	return releaseKey
}

func (dm *DistributedMutex) MustAcquireLockWithSubKey(subKey string, interval time.Duration, ttl ...time.Duration) string {
	var (
		releaseKey string
		success    = false
	)
	if interval < 50*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	for {
		releaseKey, success = dm.AcquireLockWithSubKey(subKey, ttl...)
		if success {
			break
		}
		time.Sleep(interval)
	}
	return releaseKey
}

// ReleaseLockWithSubKey release lock
func (dm *DistributedMutex) ReleaseLockWithSubKey(subKey string, releaseKey string) bool {
	luaScript := `
		if redis.call("EXISTS", KEYS[1]) == 0 then
			return 1
		end
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`
	result, err := dm.client.Eval(context.Background(), luaScript, []string{dm.getKey(subKey)}, releaseKey).Result()
	if err != nil {
		return false
	}
	return result.(int64) > 0
}

func (dm *DistributedMutex) MustReleaseLockWithSubKey(subKey string, releaseKey string) {
	success := false
	for {
		success = dm.ReleaseLockWithSubKey(subKey, releaseKey)
		if success {
			break
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (dm *DistributedMutex) ReleaseLock(releaseKey string) bool {
	return dm.ReleaseLockWithSubKey("", releaseKey)
}

func (dm *DistributedMutex) MustReleaseLock(releaseKey string) {
	success := false
	for {
		success = dm.ReleaseLock(releaseKey)
		if success {
			break
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
}
