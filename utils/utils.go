package utils

import (
	"math"
	"time"
)

/*
Retries a function a specified number of times while exponentially increasing the sleep time between each retry. The
time to sleep between requests is calculated by: 2^(retry - 1) * sleep coefficient.

Example of using this method on a function that fails 5 times with a sleep coefficient of 1 second
	Attempt 1
		No pause
	Attempt 2
		pause = 2^(1 - 1) * 1s = 1s
	Attempt 3
		pause = 2^(2 - 1) * 1s = 2s
	Attempt 4
		pause = 2^(3 - 1) * 1s = 4s
	Attempt 5
		pause = 2^(4 - 1) * 1s = 8s
*/
func Retry(fn func() error, maxRetries int, sleep time.Duration) error {
	return retryWithExponentialBackOff(fn, 0, maxRetries, sleep)
}

func ExponentialBackOffFromRetryCount(retry int, sleep time.Duration) time.Duration {
	return time.Duration(math.Pow(2, float64(retry-1))) * sleep
}

func retryWithExponentialBackOff(fn func() error, retry int, maxRetries int, sleep time.Duration) error {
	if err := fn(); err != nil {
		if retry++; retry < maxRetries {
			time.Sleep(ExponentialBackOffFromRetryCount(retry, sleep))
			return retryWithExponentialBackOff(fn, retry, maxRetries, sleep)
		} else {
			return err
		}
	} else {
		return nil
	}
}
