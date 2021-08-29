package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibm-functions/cos-proxy/logger"
	"github.com/ibm-functions/cos-proxy/utils"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	timeout             = 10 * time.Minute // Max round-trip time for request
	tlsHandshakeTimeout = 10 * time.Second // Time to wait for TLS handshake
)

type Proxy struct {
	httpClient        *http.Client
	kubeClient        *kubernetes.Clientset
	logger            *logger.Logger
	proxyName         string
	proxyNamespace    string
	proxyOrdinal      int64
	proxyStatefulSet  string
	retryCache        RetryCache
	retryTimeToLive   time.Duration
	retriesToCache    []interface{}
	retriesToCacheMux sync.Mutex
}

type RetryCache interface {
	Add(value ...interface{}) error
}

type RetryEntry struct {
	Recipient     string `json:"recipient"`
	Origination   int64  `json:"origination"`
	MsgKey        []byte `json:"msgKey"`
	MsgValue      []byte `json:"msgValue"`
	NextRetryTime int64  `json:"nextRetryTime"`
	Retries       int    `json:"retries"`
}

type CosPayload struct {
	Notification json.RawMessage `json:"notification"`
}

type CosMsgKey struct {
	Format         string   `json:"format"`
	RequestId      string   `json:"request_id"`
	NotificationId string   `json:"notification_id"`
	Recipients     []string `json:"recipients"`
}

// Info of StatefulSet
var proxies struct {
	Count   int64
	CountMu sync.Mutex
	List    struct {
		sync.RWMutex
		IPs     string
		Version string
	}
}

// State of the current proxy
var state struct {
	ActiveRequests   int64
	ActiveRequestsMu sync.Mutex
	RequestCounter   uint64
	DenyCounter      uint64
	IdleShutdown     struct {
		sync.RWMutex
		LastTime time.Time
	}
}

// Config from annotations (+ readiness probe)
var config struct {
	MinProxies    int64
	MaxProxies    int64
	MaxRequests   int64
	MaxLoadFactor float64
	ProxyTimeout  int64
	IdleTimeout   int64

	// HTTP config comes from readiness probe
	HTTP struct {
		Path string
		Port int
	}
}

func NewProxy(retryCache RetryCache) (*Proxy, error) {
	var proxy *Proxy

	config, err := GetConfig()
	if err != nil {
		return nil, err
	}
	proxy = &Proxy{}
	proxy.logger = logger.GetLogger()
	proxy.proxyName = config.ProxyName
	proxy.proxyNamespace = config.ProxyNamespace
	proxy.proxyStatefulSet = config.ProxyStatefulSet
	proxy.proxyOrdinal = getProxyOrdinal(proxy.proxyName, proxy.proxyStatefulSet)

	netHTTPTransport := &http.Transport{
		TLSHandshakeTimeout: tlsHandshakeTimeout,
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
	}
	proxy.httpClient = &http.Client{
		Transport: netHTTPTransport,
		Timeout:   timeout,
	}
	proxy.retryCache = retryCache
	proxy.retryTimeToLive = time.Duration(config.RetryTimeToLive) * time.Second

	go proxy.cacheRetries()

	return proxy, nil
}

// StartProxy starts the HTTP server
func (p *Proxy) StartProxy() error {
	p.startWatcher()
	p.setupIdleShutdown()
	http.HandleFunc(config.HTTP.Path, p.httpHandler)

	p.logger.Info("Server up and listening")
	// Start the server
	err := http.ListenAndServe(fmt.Sprintf(":%v", config.HTTP.Port), nil)

	return err
}

// Starts a watcher for modifications to the StatefulSet and pods
func (p *Proxy) startWatcher() {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Error getting the cluster config: %s", err)
	}

	p.kubeClient, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error setting up KubeClient: %s", err)
	}

	// Do an initial StatefulSet query
	statefulSet, err := p.kubeClient.AppsV1().StatefulSets(p.proxyNamespace).Get(context.Background(), p.proxyStatefulSet, metav1.GetOptions{})
	if err != nil {
		log.Fatalf("Error querying initial StatefulSet: %v", err)
	}

	p.updateStatefulSet(statefulSet)

	// Watches the StatefulSet events
	go func() {
		for {
			setWatcher, err := p.kubeClient.AppsV1().StatefulSets(p.proxyNamespace).Watch(context.Background(), metav1.ListOptions{
				LabelSelector: "component=" + p.proxyStatefulSet,
				Watch:         true,
			})

			if err != nil {
				log.Fatalf("Error setting up the watcher: %v", err)
			}

			for {
				event := <-setWatcher.ResultChan()
				if event.Type == "" {
					break
				}

				if event.Type == watch.Added || event.Type == watch.Modified {
					p.updateStatefulSet(event.Object.(*v1.StatefulSet))
				}
			}
		}
	}()
}

// Sets up the idle shutdown timer
func (p *Proxy) setupIdleShutdown() {
	// Never scale to 0
	if p.proxyOrdinal == 0 {
		return
	}

	p.resetIdleShutdown()

	// Waits for the idle shutdown timer to finish
	go func() {
		for {
			if p.shouldDoIdleShutdown() {
				state.IdleShutdown.Lock()

				if p.shouldDoIdleShutdown() && p.scaleDown() {
					// Not necessary, but this proxy is going to shutdown anyway
					os.Exit(0)
				}

				state.IdleShutdown.Unlock()
			}
			time.Sleep(time.Second)
		}
	}()
}

// Proxy's HTTP handler
func (p *Proxy) httpHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Handle ensure requests
	if p.handleEnsureRequest(w, r) {
		return
	}

	// Forward-To is the host to forward the request to
	forwardTo := strings.TrimSpace(r.Header.Get("Forward-To"))

	// Is there no Forward-To header?
	if forwardTo == "" {
		// If so, return metrics.
		p.writeProxyMetrics(w, http.StatusOK)
		return
	}

	// Have we fully maxed out?
	if state.ActiveRequests >= int64(config.MaxRequests) {
		// If so, deny the request and return metrics
		p.writeProxyMetrics(w, http.StatusTooManyRequests)
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	// Perform a double check after locking
	state.ActiveRequestsMu.Lock()
	if state.ActiveRequests >= int64(config.MaxRequests) {
		state.ActiveRequestsMu.Unlock()
		p.writeProxyMetrics(w, http.StatusTooManyRequests)
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	// Increase the active request count
	atomic.AddInt64(&state.ActiveRequests, 1)

	state.ActiveRequestsMu.Unlock()

	// Delete the Forward-To header for when we copy the request to proxy it
	r.Header.Del("Forward-To")

	adapterURL, err := url.Parse(forwardTo)
	if err != nil {
		p.logger.Error("Failed to parse adapter URL.",
			zap.String("url", forwardTo),
			zap.Error(err))
	}
	name := adapterURL.Path

	// Read the body to copy it
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		p.logger.Error("Failed to read request body.",
			zap.String("name", name),
			zap.Error(err))

		atomic.AddInt64(&state.ActiveRequests, -1)
		p.writeProxyMetrics(w, http.StatusInternalServerError)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var cosPayload CosPayload
	if err := json.Unmarshal(body, &cosPayload); err != nil {
		p.logger.Error("Failed to unmarshal payload.",
			zap.String("name", name),
			zap.Error(err))

		atomic.AddInt64(&state.ActiveRequests, -1)
		p.writeProxyMetrics(w, http.StatusInternalServerError)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	retryEntry, cosMsgKey, err := p.createRetryEntry(r, cosPayload.Notification)
	if err != nil {
		p.logger.Error("Failed to create retry entry.",
			zap.String("name", name),
			zap.Error(err))

		atomic.AddInt64(&state.ActiveRequests, -1)
		p.writeProxyMetrics(w, http.StatusInternalServerError)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Create the proxy request
	proxyRequest, err := http.NewRequest(r.Method, forwardTo, bytes.NewReader(body))
	if err != nil {
		p.logger.Error("Failed to create proxy request.",
			zap.String("name", name),
			zap.Error(err))

		atomic.AddInt64(&state.ActiveRequests, -1)
		p.writeProxyMetrics(w, http.StatusInternalServerError)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Copy the headers
	proxyRequest.Header = r.Header

	// Start the request
	go func() {
		defer func() {
			// Restart the timer
			p.resetIdleShutdown()

			// Decrement the current number of active requests
			atomic.AddInt64(&state.ActiveRequests, -1)
		}()

		p.logger.Debug("Sending event to adapter...",
			zap.String("name", name),
			zap.String("notificationId", cosMsgKey.NotificationId),
			zap.String("recipient", retryEntry.Recipient),
			zap.String("requestId", cosMsgKey.RequestId))

		//post event message to COS adapter
		res, body, err := p.doRequest(proxyRequest)
		if err != nil || shouldRetry(res.StatusCode, res.Header) {
			if err != nil {
				p.logger.Warn("COS adapter request error.",
					zap.String("name", name),
					zap.String("notificationId", cosMsgKey.NotificationId),
					zap.String("recipient", retryEntry.Recipient),
					zap.String("requestId", cosMsgKey.RequestId),
					zap.Int("retries", retryEntry.Retries),
					zap.Error(err))
			} else {
				p.logger.Warn("COS adapter response retry code.",
					zap.String("name", name),
					zap.String("reason", string(body)),
					zap.Int("statusCode", res.StatusCode),
					zap.String("notificationId", cosMsgKey.NotificationId),
					zap.String("recipient", retryEntry.Recipient),
					zap.String("requestId", cosMsgKey.RequestId),
					zap.Int("retries", retryEntry.Retries),
					zap.Any("headers", res.Header))
			}
			if retryEntry.Retries > 0 {
				go p.updateRetry(retryEntry, cosMsgKey, name)
			} else {
				go p.addRetry(retryEntry, cosMsgKey, name)
			}
		} else if res.StatusCode != http.StatusOK {
			p.logger.Error("COS adapter response failure code.",
				zap.String("name", name),
				zap.String("reason", string(body)),
				zap.Int("statusCode", res.StatusCode),
				zap.String("notificationId", cosMsgKey.NotificationId),
				zap.String("recipient", retryEntry.Recipient),
				zap.String("requestId", cosMsgKey.RequestId),
				zap.Any("headers", res.Header))
		}
	}()

	p.writeProxyMetrics(w, http.StatusAccepted)
	w.WriteHeader(http.StatusAccepted)
}

func (p *Proxy) createRetryEntry(r *http.Request, cosPayload []byte) (*RetryEntry, *CosMsgKey, error) {
	retries, err := strconv.Atoi(r.Header.Get("Retries"))
	if err != nil {
		p.logger.Error("Error parsing Retries header.", zap.Error(err))
		return nil, nil, err
	}
	origination, err := strconv.ParseInt(r.Header.Get("Origination"), 10, 64)
	if err != nil {
		p.logger.Error("Error parsing Origination header.", zap.Error(err))
		return nil, nil, err
	}
	recipient := r.Header.Get("Recipient")
	notificationId := r.Header.Get("NotificationId")
	requestId := r.Header.Get("RequestId")
	cosMsgKey := CosMsgKey{Format: "2.0", RequestId: requestId, NotificationId: notificationId, Recipients: []string{recipient}}

	msgKey, err := json.Marshal(&cosMsgKey)
	if err != nil {
		p.logger.Error("Error marshaling COS msg key.", zap.Error(err))
		return nil, nil, err
	}
	retryEntry := RetryEntry{MsgKey: msgKey, MsgValue: cosPayload, Recipient: recipient, Origination: origination, Retries: retries}

	return &retryEntry, &cosMsgKey, nil
}

func (p *Proxy) cacheRetries() {
	for {
		p.retriesToCacheMux.Lock()
		if len(p.retriesToCache) > 0 {
			p.logger.Debug("Storing entries in retry cache.")
			if err := utils.Retry(func() error {
				if err := p.retryCache.Add(p.retriesToCache...); err != nil {
					p.logger.Error("Failed to add entries in retry cache.", zap.Error(err))
					return err
				}
				return nil
			}, 5, time.Second); err != nil {
				p.logger.Error("Exhausted all attempts to add entries to retry cache.", zap.Error(err))
			}
			p.retriesToCache = nil
		}
		p.retriesToCacheMux.Unlock()
		time.Sleep(time.Second)
	}
}

func (p *Proxy) addRetry(newRetry *RetryEntry, cosMsgKey *CosMsgKey, name string) {
	// Initial retries we occur after 5 seconds
	sleepDuration := 5 * time.Second
	newRetry.NextRetryTime = time.Now().UTC().Add(sleepDuration).Unix()
	newRetry.Retries++

	p.logger.Debug("Adding event to retry queue.",
		zap.String("name", name),
		zap.String("recipient", newRetry.Recipient),
		zap.String("requestId", cosMsgKey.RequestId),
		zap.String("notificationId", cosMsgKey.NotificationId),
		zap.Duration("sleepDuration", sleepDuration),
		zap.Int64("nextRetryTime", newRetry.NextRetryTime))

	if buf, err := json.Marshal(&newRetry); err != nil {
		p.logger.Error("Failed to marshal initial retry entry.", zap.Any("entry", newRetry))
	} else {
		// Update the entries to add to the retry cache with the next retry time as score and then add the key with
		// buffer as the member to be retried
		p.retriesToCacheMux.Lock()
		p.retriesToCache = append(p.retriesToCache, newRetry.NextRetryTime)
		p.retriesToCache = append(p.retriesToCache, buf)
		p.retriesToCacheMux.Unlock()
	}
}

func (p *Proxy) updateRetry(updateRetry *RetryEntry, cosMsgKey *CosMsgKey, name string) {
	// After an initial retry, triggers will be retried using exponential backoff with a sleep coefficient of 1 second
	sleepDuration := utils.ExponentialBackOffFromRetryCount(updateRetry.Retries, time.Second)
	nextRetryTime := time.Now().UTC().Add(sleepDuration)

	if nextRetryTime.Sub(time.Unix(updateRetry.Origination, 0)).Seconds() < p.retryTimeToLive.Seconds() {
		updateRetry.NextRetryTime = nextRetryTime.Unix()
		updateRetry.Retries++

		p.logger.Debug("Previous retry failed, adding event back to retry queue.",
			zap.String("name", name),
			zap.String("recipient", updateRetry.Recipient),
			zap.String("requestId", cosMsgKey.RequestId),
			zap.String("notificationId", cosMsgKey.NotificationId),
			zap.Duration("sleepDuration", sleepDuration),
			zap.Int64("nextRetryTime", updateRetry.NextRetryTime))

		if buf, err := json.Marshal(&updateRetry); err != nil {
			p.logger.Error("Failed to marshal updated retry entry.", zap.Any("entry", updateRetry))
		} else {
			// Update the entries to add to the retry cache with the next retry time as score and then add the key with
			// buffer as the member to be retried
			p.retriesToCacheMux.Lock()
			p.retriesToCache = append(p.retriesToCache, updateRetry.NextRetryTime)
			p.retriesToCache = append(p.retriesToCache, buf)
			p.retriesToCacheMux.Unlock()
		}
	} else {
		p.logger.Error("Max retry time exceeded, dropping event.",
			zap.String("name", name),
			zap.String("recipient", updateRetry.Recipient),
			zap.String("requestId", cosMsgKey.RequestId),
			zap.String("notificationId", cosMsgKey.NotificationId))
	}
}

func (p *Proxy) doRequest(req *http.Request) (*http.Response, []byte, error) {
	res, err := p.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)

	return res, body, err
}

// Handles an ensure request if it exists, returns false if none exists
func (p *Proxy) handleEnsureRequest(w http.ResponseWriter, r *http.Request) bool {
	ensure := strings.TrimSpace(r.Header.Get("Ensure-Requests"))
	if ensure == "" {
		return false
	}

	// Ensure-Requests are the number of requests to expect
	ensureRequests, err := strconv.ParseUint(ensure, 10, 64)
	if err != nil {
		p.writeProxyMetrics(w, http.StatusInternalServerError)
		return true
	}

	// Determine how many proxies are needed based on the ideal load amount for each proxy
	// Why does Go not have a min that works with ints?
	desiredProxyCount := int64(math.Min(float64(config.MaxProxies), float64(int64(ensureRequests)/int64(float64(config.MaxRequests)*config.MaxLoadFactor))))

	// Scale up, if necessary
	if proxies.Count < desiredProxyCount {
		proxies.CountMu.Lock()

		if proxies.Count < desiredProxyCount {
			p.scaleStatefulSet(int(desiredProxyCount))
		}

		proxies.CountMu.Unlock()
	}

	p.writeProxyMetrics(w, http.StatusOK)
	return true
}

// Changes the StatefulSet replica count
func (p *Proxy) scaleStatefulSet(newScale int) {
	p.logger.Debug("Attempting to scale proxies", zap.Int("scaleCount", newScale))

	// Cap it at max proxies
	if int64(newScale) > config.MaxProxies {
		newScale = int(config.MaxProxies)
	}

	// Skip useless scalings
	if int64(newScale) == proxies.Count || int64(newScale) < config.MinProxies {
		return
	}

	retries := 5
	for retry := 0; retry < retries; retry++ {
		statefulSets := p.kubeClient.AppsV1().StatefulSets(p.proxyNamespace)

		scale, err := statefulSets.GetScale(context.Background(), p.proxyStatefulSet, metav1.GetOptions{})
		if err != nil {
			log.Fatalf("Error getting StatefulSet's scale: %s", err)
		}

		scale.Spec.Replicas = int32(newScale)
		scale.Status.Replicas = int32(newScale)

		scale, err = statefulSets.UpdateScale(context.Background(), p.proxyStatefulSet, scale, metav1.UpdateOptions{})
		if err != nil {
			p.logger.Debug("Error updating StatefulSet's scale", zap.Int("retryCount", retry), zap.Error(err))
			continue
		}

		proxies.Count = int64(newScale)
		return
	}

	log.Fatalf("Failed to scale up after %v tries", retries)
}

// Scales up if we are the last proxy and have not hit the max proxies
func (p *Proxy) scaleUp() bool {
	if proxies.Count+1 <= config.MaxProxies && p.proxyOrdinal+1 == proxies.Count {
		proxies.CountMu.Lock()
		defer proxies.CountMu.Unlock()

		if proxies.Count+1 <= config.MaxProxies && p.proxyOrdinal+1 == proxies.Count {
			p.scaleStatefulSet(int(p.proxyOrdinal) + 2)
			return true
		}
	}

	return false
}

// Scales down if we are the last proxy and have not hit the min proxies
func (p *Proxy) scaleDown() bool {
	if proxies.Count-1 >= config.MinProxies && p.proxyOrdinal+1 == proxies.Count {
		proxies.CountMu.Lock()
		defer proxies.CountMu.Unlock()

		if proxies.Count-1 >= config.MinProxies && p.proxyOrdinal+1 == proxies.Count {
			p.scaleStatefulSet(int(p.proxyOrdinal))
			return true
		}
	}

	return false
}

// Writes the current proxy's metrics to response writer
func (p *Proxy) writeProxyMetrics(w http.ResponseWriter, proxyStatus int) {
	if proxyStatus == http.StatusTooManyRequests {
		atomic.AddUint64(&state.DenyCounter, 1)
	}

	free := int(float64(config.MaxRequests)*config.MaxLoadFactor) - int(state.ActiveRequests)

	// If we have no more free requests based on load factor, try to scale up
	if free <= 0 {
		go p.scaleUp()
	}

	w.Header().Set("Proxy-Counter", strconv.Itoa(int(atomic.AddUint64(&state.RequestCounter, 1))))
	w.Header().Set("Proxy-Free", strconv.Itoa(free))
	w.Header().Set("Proxy-Ordinal", strconv.Itoa(int(p.proxyOrdinal)))
	w.Header().Set("Proxy-Status", strconv.Itoa(proxyStatus))

	proxies.List.RLock()
	w.Header().Set("Proxy-Version", proxies.List.Version)
	w.Header().Set("Proxy-List", proxies.List.IPs)
	proxies.List.RUnlock()
}

// Resets the idle shutdown timer if applicable
func (p *Proxy) resetIdleShutdown() {
	state.IdleShutdown.LastTime = time.Now()
}

// Should we do an idle shutdown?
func (p *Proxy) shouldDoIdleShutdown() bool {
	return p.proxyOrdinal != 0 && p.proxyOrdinal >= config.MinProxies && state.ActiveRequests == 0 && time.Since(state.IdleShutdown.LastTime) >= time.Duration(config.IdleTimeout)*time.Second
}

// Updates the HTTP config from any containers valid readiness probe
func (p *Proxy) updateHTTPConfig(containers []corev1.Container) error {
	// Don't update if already valid
	if config.HTTP.Port != 0 {
		return nil
	}

	for _, c := range containers {
		if readinessProbe := c.ReadinessProbe; readinessProbe != nil {
			if httpGet := readinessProbe.HTTPGet; httpGet != nil {
				config.HTTP.Path = httpGet.Path
				config.HTTP.Port = httpGet.Port.IntValue()
				break
			}
		}
	}

	if config.HTTP.Port == 0 {
		return fmt.Errorf("found no valid HTTP get readiness probe in container spec")
	}

	if config.HTTP.Path == "" {
		config.HTTP.Path = "/"
	}

	return nil
}

// Updates the proxy list from the StatefulSet
func (p *Proxy) updateProxyList(set *v1.StatefulSet) error {
	// Get the pods that are part of the current StatefulSet
	podList, err := p.kubeClient.CoreV1().Pods(p.proxyNamespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "component=" + p.proxyStatefulSet,
	})

	if err != nil {
		return err
	}

	if len(podList.Items) == 0 {
		return fmt.Errorf("found no pods in the StatefulSet %v", p.proxyStatefulSet)
	}

	// Sort the proxies in increasing ordinal number
	sort.Slice(podList.Items, func(i, j int) bool {
		podA := podList.Items[i]
		podB := podList.Items[j]

		return getProxyOrdinal(podA.Name, p.proxyStatefulSet) < getProxyOrdinal(podB.Name, p.proxyStatefulSet)
	})

	if err := p.updateHTTPConfig(podList.Items[0].Spec.Containers); err != nil {
		return err
	}

	// Determine which pods are ready
	var newProxyList strings.Builder
	newProxyList.WriteRune('{')

	// Construct the list of pods that are running and pass the readiness check
	for ordinal, pod := range podList.Items {
		readinessCheck := false

		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				readinessCheck = true
				break
			}
		}

		if readinessCheck && pod.Status.Phase == corev1.PodRunning {
			if newProxyList.Len() != 1 {
				newProxyList.WriteRune(',')
			}

			newProxyList.WriteString(fmt.Sprintf(`"%v":"%v"`, ordinal, pod.Status.PodIP))
		}
	}

	newProxyList.WriteRune('}')

	// Update the active proxies list
	proxies.List.Lock()
	proxies.List.IPs = newProxyList.String()
	proxies.List.Version = set.ObjectMeta.ResourceVersion
	proxies.List.Unlock()

	// Update the number of intended proxies
	newProxyCount := int64(*set.Spec.Replicas)

	// Begin shared lock for idle shutdown
	state.IdleShutdown.RLock()
	if oldProxyCount := atomic.SwapInt64(&proxies.Count, int64(newProxyCount)); oldProxyCount != newProxyCount {
		// If we downscaled, retrigger shutdown timer
		if oldProxyCount > newProxyCount {
			p.resetIdleShutdown()
		}
		p.logger.Debug("New proxy count", zap.Int64("proxyCount", proxies.Count))
	}
	state.IdleShutdown.RUnlock()

	return nil
}

func (p *Proxy) getOptionalConfigValue(annotations map[string]string, configName string, defaultValue uint64) (uint64, error) {
	stringValue, ok := annotations[configName]
	stringValue = strings.TrimSpace(stringValue)

	if !ok || stringValue == "" {
		p.logger.Info("Defaulting config value", zap.String("configName", configName), zap.Uint64("defaultValue", defaultValue))
		return defaultValue, nil
	}

	value, err := strconv.ParseUint(stringValue, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%v was not properly defined: %v", configName, err)
	}

	return value, nil
}

func (p *Proxy) getOptionalConfigValueFloat(annotations map[string]string, configName string, defaultValue float64) (float64, error) {
	stringValue, ok := annotations[configName]
	stringValue = strings.TrimSpace(stringValue)

	if !ok || stringValue == "" {
		p.logger.Info("Defaulting config value", zap.String("configName", configName), zap.Float64("defaultValue", defaultValue))
		return defaultValue, nil
	}

	value, err := strconv.ParseFloat(stringValue, 64)
	if err != nil {
		return 0, fmt.Errorf("%v was not properly defined: %v", configName, err)
	}

	return value, nil
}

// Updates the proxy config from the annotations
func (p *Proxy) updateProxyConfig(annotations map[string]string) error {
	// config.MinProxies is the minimum number of proxy pods
	newMinProxies, err := p.getOptionalConfigValue(annotations, "minProxies", 1)
	if err != nil {
		return err
	}

	// config.MinProxies must be > 0
	if newMinProxies == 0 {
		return fmt.Errorf("minProxies must be > 0: %v", err)
	}

	// config.MaxProxies is the maximum number of proxy pods
	newMaxProxies, err := p.getOptionalConfigValue(annotations, "maxProxies", math.MaxInt64)
	if err != nil {
		return err
	}

	// config.MaxProxies must be >= config.MinProxies
	if newMaxProxies < newMinProxies {
		return fmt.Errorf("maxProxies must be >= minProxies: %v was not >= %v", newMaxProxies, newMinProxies)
	}

	// config.MaxRequests represents the actual maximum requests a proxy can handle
	newMaxRequests, err := p.getOptionalConfigValue(annotations, "maxRequests", 100)
	if err != nil {
		return err
	}

	// config.MaxLoadFactor represents the ideal max requests a proxy should handle (target = config.MaxLoadFactor * config.MaxRequests)
	newMaxLoadFactor, err := p.getOptionalConfigValueFloat(annotations, "maxLoadFactor", 0.5)
	if err != nil {
		return err
	}

	// config.ProxyTimeout is timeout in milliseconds the proxy waits for a response from the original host before moving on
	newProxyTimeout, err := p.getOptionalConfigValue(annotations, "proxyTimeout", 100)
	if err != nil {
		return err
	}

	// config.IdleTimeout is time in seconds the proxy waits for to shutdown after no activity
	newIdleTimeout, err := p.getOptionalConfigValue(annotations, "idleTimeout", 10)
	if err != nil {
		return err
	}

	// Begin shared lock for idle shutdown
	state.IdleShutdown.RLock()
	defer state.IdleShutdown.RUnlock()

	// Determine if we should restart the idle timer
	if int64(newIdleTimeout) != config.IdleTimeout || (p.proxyOrdinal > int64(newMinProxies) && p.proxyOrdinal <= config.MinProxies) {
		p.resetIdleShutdown()
	}

	// Update the config globals
	config.MinProxies = int64(newMinProxies)
	config.MaxProxies = int64(newMaxProxies)
	config.MaxRequests = int64(newMaxRequests)
	config.MaxLoadFactor = newMaxLoadFactor
	config.ProxyTimeout = int64(newProxyTimeout)
	config.IdleTimeout = int64(newIdleTimeout)

	// If we are the last proxy, ensure the min/max number of proxies
	if p.proxyOrdinal+1 == proxies.Count {
		proxies.CountMu.Lock()

		// Double check after locking
		if p.proxyOrdinal+1 == proxies.Count {
			if proxies.Count < config.MinProxies {
				p.scaleStatefulSet(int(config.MinProxies))
			} else if proxies.Count > config.MaxProxies {
				p.scaleStatefulSet(int(config.MaxProxies))
			}
		}

		proxies.CountMu.Unlock()
	}

	return nil
}

// Updates the global info regarding the StatefulSet
func (p *Proxy) updateStatefulSet(set *v1.StatefulSet) {
	// Update proxy list
	if err := p.updateProxyList(set); err != nil {
		log.Fatalf("Failed to update proxy list: %v", err)
	}

	// Update configs
	if err := p.updateProxyConfig(set.GetObjectMeta().GetAnnotations()); err != nil {
		log.Fatalf("Failed to update proxy config: %v", err)
	}
}

// Returns the proxy's ordinal, which represents the proxy's current index in the StatefulSet
func getProxyOrdinal(podName string, proxyStatefulSet string) int64 {
	value, err := strconv.ParseUint(strings.Replace(podName, proxyStatefulSet+"-", "", 1), 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse proxy ordinal in %s: %v", podName, err)
	}

	return int64(value)
}

func shouldRetry(statusCode int, header http.Header) bool {
	_, headerExists := header["COS-Adapter-Served"]

	return (statusCode == http.StatusNotFound && !headerExists) ||
		statusCode == http.StatusTooManyRequests ||
		statusCode == http.StatusInternalServerError ||
		statusCode == http.StatusBadGateway ||
		statusCode == http.StatusServiceUnavailable ||
		statusCode == http.StatusGatewayTimeout
}
