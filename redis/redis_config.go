package redis

import (
	"github.com/kelseyhightower/envconfig"
)

type RedisConfig struct {
	RedisURL string `required:"true" envconfig:"cos_proxy_env_redis_url"`
	RedisKey string `required:"true" envconfig:"cos_proxy_env_redis_key"`
}

var redisConfig *RedisConfig // Singleton instance

// Return a copy, so that the singleton config can't be touched
func GetConfig() (RedisConfig, error) {
	var err error

	if redisConfig == nil {
		redisConfig, err = getConfig()
	}

	return *redisConfig, err
}

// Read in the configuration from the environment variables
func getConfig() (*RedisConfig, error) {
	var cfg RedisConfig
	err := envconfig.Process("COS_PROXY_ENV", &cfg)
	return &cfg, err
}
