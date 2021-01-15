package logger

import (
	"github.com/kelseyhightower/envconfig"
)

type LogConfig struct {
	Level string `required:"true" envconfig:"cos_proxy_env_log_level"`
}

var logConfig *LogConfig // Singleton instance

func GetConfig() *LogConfig {
	var err error

	if logConfig == nil {
		logConfig, err = getConfig()

		if err != nil {
			panic(err)
		}
	}

	return logConfig
}

func getConfig() (*LogConfig, error) {
	var cfg LogConfig
	err := envconfig.Process("COS_PROXY_ENV", &cfg)
	return &cfg, err
}
