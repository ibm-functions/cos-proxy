package redis

import (
	"crypto/tls"
	"github.com/gomodule/redigo/redis"
	"time"
)

const (
	redisAddCmd = "ZADD"
)

type Redis struct {
	pool     *redis.Pool
	redisKey string
}

// Redis Singleton
var r *Redis

// Return a copy of the Redis singleton.
func GetRedis() (*Redis, error) {
	var err error

	if r == nil {
		r, err = initializeRedis()
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func initializeRedis() (*Redis, error) {
	var err error

	config, err := GetConfig()
	if err != nil {
		return nil, err
	}

	r := &Redis{}
	r.redisKey = config.RedisKey
	redisURL := config.RedisURL
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	r.pool = &redis.Pool{
		MaxIdle:     100,
		IdleTimeout: 60 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(redisURL, redis.DialTLSConfig(tlsConfig))
			if err != nil {
				//r.logger.Error("Redis dial failed.", zap.Error(err))
				return nil, err
			}

			//r.logger.Info("Redis connection established.",
			//	zap.Int("active", r.pool.ActiveCount()),
			//	zap.Int("idle", r.pool.IdleCount()))
			return c, err
		},
		MaxActive: 1000,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			//if err != nil {
			//r.logger.Error("Redis TestOnBorrow() failed.", zap.Error(err))
			//}
			return err
		},
		Wait: true,
	}

	return r, nil
}

func (r *Redis) Add(value ...interface{}) error {
	c := r.pool.Get()
	defer c.Close()

	args := redis.Args{r.redisKey}
	args = args.Add(value...)

	if _, err := c.Do(redisAddCmd, args...); err != nil {
		return err
	}

	return nil
}

func (r *Redis) Shutdown() {
	r.pool.Close()
}
