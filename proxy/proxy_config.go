package proxy

import (
	"github.com/kelseyhightower/envconfig"
)

type ProxyConfig struct {
	RetryTimeToLive  int    `required:"true" envconfig:"cos_proxy_env_retry_time_to_live"`
	ProxyName        string `required:"true" envconfig:"cos_proxy_env_pod_name"`
	ProxyNamespace   string `required:"true" envconfig:"cos_proxy_env_pod_namespace"`
	ProxyStatefulSet string `required:"true" envconfig:"cos_proxy_env_pod_statefulset"`
}

var proxyConfig *ProxyConfig // Singleton instance

func GetConfig() (ProxyConfig, error) {
	var err error

	if proxyConfig == nil {
		proxyConfig, err = getConfig()
	}

	return *proxyConfig, err
}

func getConfig() (*ProxyConfig, error) {
	var cfg ProxyConfig
	err := envconfig.Process("COS_PROXY_ENV", &cfg)
	return &cfg, err
}
