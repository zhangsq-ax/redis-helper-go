package redis_helper

import (
	"context"
	"github.com/redis/go-redis/v9"
	"strconv"
	"strings"
)

func GetRedisClient(opts *redis.Options) (*redis.Client, error) {
	client := redis.NewClient(opts)
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func ServerVersion(client *redis.Client) (string, error) {
	serverInfo, err := client.Info(context.Background(), "server").Result()
	if err != nil {
		return "", err
	}
	lines := strings.Split(serverInfo, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimPrefix(line, "redis_version:"), nil
		}
	}
	return "unknown", nil
}

func ServerMajorVersion(client *redis.Client) (int, error) {
	version, err := ServerVersion(client)
	if err != nil {
		return 0, nil
	}
	versionParts := strings.Split(version, ".")
	majorVer, err := strconv.ParseInt(versionParts[0], 10, 64)
	if err != nil {
		return 0, err
	}
	return int(majorVer), nil
}
