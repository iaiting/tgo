package tgo

import (
	"sync"
)

var (
	cacheConfigMux sync.Mutex
	cacheConfig    *ConfigCache
)

type ConfigCache struct {
	Redis   ConfigCacheRedis
	RedisP  ConfigCacheRedis // 持久化Redis
	Dynamic ConfigCacheDynamic
}

type ConfigCacheRedis struct {
	Address         []string
	Prefix          string
	Expire          int
	ReadTimeout     int
	WriteTimeout    int
	ConnectTimeout  int
	PoolMaxIdle     int
	PoolMaxActive   int
	PoolIdleTimeout int
	PoolMinActive   int
	Password        string
}

type ConfigCacheDynamic struct {
	DynamicAddress string
	IsDynamic      bool
	CycleTime      int
}

func configCacheGet() (err error) {
	if cacheConfig == nil || len(cacheConfig.Redis.Address) == 0 {
		cacheConfigMux.Lock()
		defer cacheConfigMux.Unlock()
		cacheConfig = new(ConfigCache)
		return configGet("cache", cacheConfig, nil)
	}
	return
}

func configCacheReload() {
	cacheConfigMux.Lock()
	defer cacheConfigMux.Unlock()
	cacheConfig = nil
	configCacheGet()
}

func ConfigCacheGetRedis() *ConfigCacheRedis {
	configCacheGet()
	if cacheConfig == nil {
		return new(ConfigCacheRedis)
	}
	return &cacheConfig.Redis
}

func ConfigCacheGetRedisWithConn(persistent bool) *ConfigCacheRedis {
	configCacheGet()
	var redisConfig ConfigCacheRedis
	if !persistent {
		redisConfig = cacheConfig.Redis
	} else {
		redisConfig = cacheConfig.RedisP
	}
	return &redisConfig
}

func ConfigCacheGetRedisDynamic() *ConfigCacheDynamic {
	configCacheGet()
	return &cacheConfig.Dynamic
}
