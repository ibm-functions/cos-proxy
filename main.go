package main

import (
	"github.com/ibm-functions/cos-proxy/logger"
	"github.com/ibm-functions/cos-proxy/proxy"
	"github.com/ibm-functions/cos-proxy/redis"
	"go.uber.org/zap"
)

func main() {
	proxyLogger := logger.GetLogger()
	proxyRedis, err := redis.GetRedis()
	if err != nil {
		panic(err)
	}

	newProxy, err := proxy.NewProxy(proxyRedis)
	if err != nil {
		proxyLogger.Error("Failed to create proxy.", zap.Error(err))
		panic(err)
	}

	if err := newProxy.StartProxy(); err != nil {
		proxyLogger.Error("Proxy server failed.", zap.Error(err))
		panic(err)
	}
}
