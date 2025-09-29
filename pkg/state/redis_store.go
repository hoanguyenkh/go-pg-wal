package state

import (
	"context"
	"fmt"

	"github.com/KyberNetwork/kutils/cache"
	"github.com/jackc/pglogrepl"
)

type RedisStore struct {
	redisCache *cache.RedisCache
	keyPrefix  string
}

func (r *RedisStore) cacheKey(key string) string {
	return fmt.Sprintf("%s_%s", r.keyPrefix, key)
}

func (r *RedisStore) SaveLSN(ctx context.Context, key string, lsn pglogrepl.LSN) error {
	return r.redisCache.Set(r.cacheKey(key), lsn, 0)
}

func (r *RedisStore) LoadLSN(ctx context.Context, key string) (pglogrepl.LSN, error) {
	var result pglogrepl.LSN
	err := r.redisCache.Get(r.cacheKey(key), &result)
	return result, err
}

func (r *RedisStore) Close() error {
	return nil
}

func NewRedisStore(redisCache *cache.RedisCache, keyPrefix string) IStateStore {
	return &RedisStore{
		redisCache: redisCache,
		keyPrefix:  keyPrefix,
	}
}
