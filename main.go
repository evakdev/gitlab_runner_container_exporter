package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/api/types"
	tcontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// cachePeriod indicates the period of time the collector will reuse the results of docker inspect.
	cachePeriod = 1 * time.Second
	metricPrefix = "gitlab_runner_container_"
	containerIDLength = 12
)

var (
	// labelSanitizerRegex is compiled once and reused for label sanitization
	labelSanitizerRegex = regexp.MustCompile("[^a-zA-Z0-9_]")
)

type dockerContainerCollector struct {
	mu                 sync.RWMutex
	containerClient    *client.Client
	containerInfoCache []types.ContainerJSON
	containerImageMap  map[string]string // Maps container ID to image name
	containerNameMap   map[string]string // Maps container ID to container name
	statsCache         map[string]*containerStats
	lastInfoSeen       time.Time
	errorLogger        log.Logger
	customLabels       map[string]string // Maps final label name to container label key
}

type containerStats struct {
	name                      string
	id                        string
	cpuUsageRatio             float64
	memoryUsageBytes          uint64
	memoryUsageRssBytes       uint64
	memoryLimitBytes          uint64
	memoryUsageRatio          float64
	networkReceivedBytes      uint64
	networkTransmittedBytes   uint64
	blockIoReadBytes          uint64
	blockIoWrittenBytes       uint64
}

type descSource struct {
	name string
	help string
}

func (desc *descSource) Desc(labels prometheus.Labels) *prometheus.Desc {
	return prometheus.NewDesc(desc.name, desc.help, nil, labels)
}

var (
	healthStatusDesc = descSource{
		metricPrefix + "health_status",
		"Container health status."}
	statusDesc = descSource{
		metricPrefix + "status",
		"Container status."}
	oomkilledDesc = descSource{
		metricPrefix + "oomkilled",
		"Container was killed by OOMKiller."}
	startedatDesc = descSource{
		metricPrefix + "startedat",
		"Time when the Container started."}
	finishedatDesc = descSource{
		metricPrefix + "finishedat",
		"Time when the Container finished."}
	restartcountDesc = descSource{
		metricPrefix + "restart_count",
		"Number of times the container has been restarted"}
	containerInfoDesc = descSource{
		metricPrefix + "info",
		"Container information with all labels."}

	// Stats metrics
	cpuUsageRatioDesc = descSource{
		metricPrefix + "cpu_usage_ratio",
		"CPU usage percentage 0-100"}
	memoryUsageBytesDesc = descSource{
		metricPrefix + "memory_usage_bytes",
		"Memory usage in bytes"}
	memoryUsageRssBytesDesc = descSource{
		metricPrefix + "memory_usage_rss_bytes",
		"Memory rss usage in bytes"}
	memoryLimitBytesDesc = descSource{
		metricPrefix + "memory_limit_bytes",
		"Memory limit in bytes"}
	memoryUsageRatioDesc = descSource{
		metricPrefix + "memory_usage_ratio",
		"Memory usage percentage 0-100"}
	networkReceivedBytesDesc = descSource{
		metricPrefix + "network_received_bytes",
		"Network received in bytes"}
	networkTransmittedBytesDesc = descSource{
		metricPrefix + "network_transmitted_bytes",
		"Network transmitted in bytes"}
	blockIoReadBytesDesc = descSource{
		metricPrefix + "blockio_read_bytes",
		"Block IO read in bytes"}
	blockIoWrittenBytesDesc = descSource{
		metricPrefix + "blockio_written_bytes",
		"Block IO written in bytes"}
)

func (c *dockerContainerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- healthStatusDesc.Desc(nil)
	ch <- statusDesc.Desc(nil)
	ch <- oomkilledDesc.Desc(nil)
	ch <- startedatDesc.Desc(nil)
	ch <- finishedatDesc.Desc(nil)
	ch <- restartcountDesc.Desc(nil)
	ch <- containerInfoDesc.Desc(nil)
	ch <- cpuUsageRatioDesc.Desc(nil)
	ch <- memoryUsageBytesDesc.Desc(nil)
	ch <- memoryUsageRssBytesDesc.Desc(nil)
	ch <- memoryLimitBytesDesc.Desc(nil)
	ch <- memoryUsageRatioDesc.Desc(nil)
	ch <- networkReceivedBytesDesc.Desc(nil)
	ch <- networkTransmittedBytesDesc.Desc(nil)
	ch <- blockIoReadBytesDesc.Desc(nil)
	ch <- blockIoWrittenBytesDesc.Desc(nil)
}

func (c *dockerContainerCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	now := time.Now()
	if now.Sub(c.lastInfoSeen) >= cachePeriod {
		if err := c.refreshContainerInfoCache(); err != nil {
			c.errorLogger.Log("message", fmt.Sprintf("Failed to refresh container info cache: %v", err))
		} else {
			c.lastInfoSeen = now
		}
	}
	c.mu.Unlock()

	c.emitContainerInfoMetrics(ch)
	c.emitContainerResourceMetrics(ch)
}

// boolToFloat64 converts a boolean to a float64 (1.0 for true, 0.0 for false)
func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

// copyLabels creates a copy of the labels map
func copyLabels(src map[string]string) prometheus.Labels {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// emitMetric is a helper function to emit a prometheus metric with proper type conversion
func emitMetric(ch chan<- prometheus.Metric, desc *descSource, labels prometheus.Labels, value interface{}) {
	var floatValue float64
	switch v := value.(type) {
	case float64:
		floatValue = v
	case uint64:
		floatValue = float64(v)
	case int:
		floatValue = float64(v)
	case int64:
		floatValue = float64(v)
	case bool:
		floatValue = boolToFloat64(v)
	default:
		// For unknown types, try to convert via float64
		floatValue = 0.0
	}
	ch <- prometheus.MustNewConstMetric(desc.Desc(labels), prometheus.GaugeValue, floatValue)
}

// truncateID truncates container ID to specified length
func truncateID(id string, length int) string {
	// Remove "sha256:" prefix if present
	id = strings.TrimPrefix(id, "sha256:")
	// Remove "/docker/" prefix if present
	id = strings.TrimPrefix(id, "/docker/")
	if len(id) > length {
		return id[:length]
	}
	return id
}

// extractJobProjectURL extracts project URL from job URL by removing '/jobs/someid' suffix
func extractJobProjectURL(jobURL string) string {
	// Remove '/jobs/...' pattern from the end
	re := regexp.MustCompile(`/jobs/[^/]+/?$`)
	return re.ReplaceAllString(jobURL, "")
}

// buildLabels builds the labels map with only id and name labels
func (c *dockerContainerCollector) buildLabels(info types.ContainerJSON) map[string]string {
	labels := make(map[string]string)
	
	// Get container ID (truncated to 12 chars, no docker/ prefix)
	containerID := truncateID(info.ID, containerIDLength)
	labels["id"] = containerID
	
	// Get container name from cache
	containerName := c.containerNameMap[info.ID]
	if containerName == "" {
		// Fallback: try to get from Name field if available
		if info.Name != "" {
			containerName = strings.TrimPrefix(info.Name, "/")
		}
	}
	labels["name"] = containerName
	
	return labels
}

// buildInfoLabels builds the labels map with all extra labels for the info metric
func (c *dockerContainerCollector) buildInfoLabels(info types.ContainerJSON) map[string]string {
	labels := make(map[string]string)
	
	// Get container ID (truncated to 12 chars, no docker/ prefix)
	containerID := truncateID(info.ID, containerIDLength)
	labels["id"] = containerID
	
	// Get container name from cache
	containerName := c.containerNameMap[info.ID]
	if containerName == "" {
		// Fallback: try to get from Name field if available
		if info.Name != "" {
			containerName = strings.TrimPrefix(info.Name, "/")
		}
	}
	labels["name"] = containerName
	
	// Get image name from cache (falls back to Config.Image if not in cache)
	imageName := c.containerImageMap[info.ID]
	if imageName == "" {
		imageName = info.Config.Image
	}
	// Remove 'docker/' prefix if present
	imageName = strings.TrimPrefix(imageName, "docker/")
	labels["image"] = imageName
	
	// Extract GitLab runner labels
	if info.Config.Labels != nil {
		if jobID, ok := info.Config.Labels["com.gitlab.gitlab-runner.job.id"]; ok {
			labels["job_id"] = jobID
		}
		if jobRef, ok := info.Config.Labels["com.gitlab.gitlab-runner.job.ref"]; ok {
			labels["job_ref"] = jobRef
		}
		if jobSHA, ok := info.Config.Labels["com.gitlab.gitlab-runner.job.sha"]; ok {
			// Truncate to 8 chars
			if len(jobSHA) > 8 {
				labels["job_commit_sha"] = jobSHA[:8]
			} else {
				labels["job_commit_sha"] = jobSHA
			}
		}
		if jobURL, ok := info.Config.Labels["com.gitlab.gitlab-runner.job.url"]; ok {
			labels["job_url_id"] = jobURL
			labels["job_project_url"] = extractJobProjectURL(jobURL)
		}
		if pipelineID, ok := info.Config.Labels["com.gitlab.gitlab-runner.pipeline.id"]; ok {
			labels["job_pipeline_id"] = pipelineID
		}
		if projectID, ok := info.Config.Labels["com.gitlab.gitlab-runner.project.id"]; ok {
			labels["job_project_id"] = projectID
		}
		if runnerID, ok := info.Config.Labels["com.gitlab.gitlab-runner.runner.id"]; ok {
			labels["job_runner_id"] = runnerID
		}
	}
	
	// Add custom labels if configured
	for finalLabelName, containerLabelKey := range c.customLabels {
		if info.Config.Labels != nil {
			if value, ok := info.Config.Labels[containerLabelKey]; ok {
				labels[finalLabelName] = value
			}
		}
	}
	
	return labels
}

// emitContainerInfoMetrics emits container information metrics (status, health, timestamps, restart count)
func (c *dockerContainerCollector) emitContainerInfoMetrics(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, info := range c.containerInfoCache {
		labels := c.buildLabels(info)
		infoLabels := c.buildInfoLabels(info)

		// Emit container info metric with all extra labels
		emitMetric(ch, &containerInfoDesc, infoLabels, 1.0)

		// Emit health status metrics
		healthStatuses := []string{"none", "starting", "healthy", "unhealthy"}
		for _, status := range healthStatuses {
			tmpLabels := copyLabels(labels)
			tmpLabels["status"] = status
			emitMetric(ch, &healthStatusDesc, tmpLabels, info.State.Health.Status == status)
		}

		// Emit container status metrics
		containerStatuses := []string{"paused", "restarting", "running", "removing", "dead", "created", "exited"}
		for _, status := range containerStatuses {
			tmpLabels := copyLabels(labels)
			tmpLabels["status"] = status
			emitMetric(ch, &statusDesc, tmpLabels, info.State.Status == status)
		}

		emitMetric(ch, &oomkilledDesc, labels, info.State.OOMKilled)

		startedAt, err := time.Parse(time.RFC3339Nano, info.State.StartedAt)
		if err != nil {
			c.errorLogger.Log("message", fmt.Sprintf("Failed to parse StartedAt timestamp: %v", err))
			continue
		}
		finishedAt, err := time.Parse(time.RFC3339Nano, info.State.FinishedAt)
		if err != nil {
			c.errorLogger.Log("message", fmt.Sprintf("Failed to parse FinishedAt timestamp: %v", err))
			continue
		}

		emitMetric(ch, &startedatDesc, labels, startedAt.Unix())
		emitMetric(ch, &finishedatDesc, labels, finishedAt.Unix())
		emitMetric(ch, &restartcountDesc, labels, info.RestartCount)
	}
}

// emitContainerResourceMetrics emits container resource usage metrics (CPU, memory, network, block IO)
func (c *dockerContainerCollector) emitContainerResourceMetrics(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, stats := range c.statsCache {
		// Ensure ID is truncated to 12 chars and has no docker/ prefix
		containerID := truncateID(stats.id, containerIDLength)
		labels := prometheus.Labels{
			"id":   containerID,
			"name": stats.name,
		}

		emitMetric(ch, &cpuUsageRatioDesc, labels, stats.cpuUsageRatio)
		emitMetric(ch, &memoryUsageBytesDesc, labels, stats.memoryUsageBytes)
		emitMetric(ch, &memoryUsageRssBytesDesc, labels, stats.memoryUsageRssBytes)
		emitMetric(ch, &memoryLimitBytesDesc, labels, stats.memoryLimitBytes)
		emitMetric(ch, &memoryUsageRatioDesc, labels, stats.memoryUsageRatio)
		emitMetric(ch, &networkReceivedBytesDesc, labels, stats.networkReceivedBytes)
		emitMetric(ch, &networkTransmittedBytesDesc, labels, stats.networkTransmittedBytes)
		emitMetric(ch, &blockIoReadBytesDesc, labels, stats.blockIoReadBytes)
		emitMetric(ch, &blockIoWrittenBytesDesc, labels, stats.blockIoWrittenBytes)
	}
}

// refreshContainerInfoCache fetches and caches container inspection data (status, health, metadata)
func (c *dockerContainerCollector) refreshContainerInfoCache() error {
	containers, err := c.containerClient.ContainerList(context.Background(), types.ContainerListOptions{All: true})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	c.containerInfoCache = make([]types.ContainerJSON, 0, len(containers))
	c.containerImageMap = make(map[string]string, len(containers))
	c.containerNameMap = make(map[string]string, len(containers))

	for _, container := range containers {
		info, err := c.containerClient.ContainerInspect(context.Background(), container.ID)
		if err != nil {
			c.errorLogger.Log("message", fmt.Sprintf("Failed to inspect container %s: %v", container.ID, err))
			continue
		}

		// Ensure Config is initialized
		if info.Config == nil {
			info.Config = &tcontainer.Config{Labels: map[string]string{}}
		}

		// Ensure Health is initialized
		if info.State.Health == nil {
			info.State.Health = &types.Health{Status: "none"}
		}

		// Store image name from container list (this is the actual image name/tag, not SHA)
		c.containerImageMap[container.ID] = container.Image

		// Store container name from container list
		if len(container.Names) > 0 {
			containerName := strings.TrimPrefix(container.Names[0], "/")
			c.containerNameMap[container.ID] = containerName
		} else if info.Name != "" {
			// Fallback to Name field from inspect
			c.containerNameMap[container.ID] = strings.TrimPrefix(info.Name, "/")
		}

		c.containerInfoCache = append(c.containerInfoCache, info)
	}
	return nil
}

// refreshContainerResourceStatsCache fetches and caches container resource usage statistics (CPU, memory, network, block IO)
func (c *dockerContainerCollector) refreshContainerResourceStatsCache() {
	containers, err := c.containerClient.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		c.errorLogger.Log("message", fmt.Sprintf("ERROR: Unable to get containers: %v", err))
		return
	}

	newStats := make(map[string]*containerStats)

	for _, container := range containers {
		stats, err := c.containerClient.ContainerStats(context.Background(), container.ID, false)
		if err != nil {
			c.errorLogger.Log("message", fmt.Sprintf("ERROR: Unable to get stats for container %s: %v", container.ID, err))
			continue
		}

		var v types.StatsJSON
		if err := json.NewDecoder(stats.Body).Decode(&v); err != nil {
			stats.Body.Close()
			c.errorLogger.Log("message", fmt.Sprintf("ERROR: Unable to decode stats for container %s: %v", container.ID, err))
			continue
		}
		stats.Body.Close()

		if len(container.Names) == 0 {
			c.errorLogger.Log("message", fmt.Sprintf("Container %s has no names, skipping", container.ID))
			continue
		}
		containerName := strings.TrimPrefix(container.Names[0], "/")
		
		// Truncate container ID to 12 chars, removing any prefixes
		containerID := truncateID(container.ID, containerIDLength)

		stat := &containerStats{
			name: containerName,
			id:   containerID,
		}

		// CPU calculation
		if v.CPUStats.CPUUsage.TotalUsage > 0 && v.PreCPUStats.CPUUsage.TotalUsage > 0 {
			cpuDelta := float64(v.CPUStats.CPUUsage.TotalUsage - v.PreCPUStats.CPUUsage.TotalUsage)
			systemDelta := float64(v.CPUStats.SystemUsage - v.PreCPUStats.SystemUsage)
			numCpus := float64(v.CPUStats.OnlineCPUs)
			if numCpus == 0 {
				numCpus = float64(len(v.CPUStats.CPUUsage.PercpuUsage))
			}
			if systemDelta > 0 {
				cpuPercent := (cpuDelta / systemDelta) * numCpus * 100.0
				stat.cpuUsageRatio = cpuPercent
			}
		}

		// Memory
		if v.MemoryStats.Usage > 0 {
			stat.memoryUsageBytes = v.MemoryStats.Usage
			if v.MemoryStats.Stats != nil {
				if rss, ok := v.MemoryStats.Stats["rss"]; ok {
					stat.memoryUsageRssBytes = rss
				}
			}
			stat.memoryLimitBytes = v.MemoryStats.Limit
			if stat.memoryLimitBytes > 0 {
				stat.memoryUsageRatio = (float64(stat.memoryUsageBytes) / float64(stat.memoryLimitBytes)) * 100.0
			}
		}

		// Network
		if len(v.Networks) > 0 {
			// Try eth0 first, then host
			if eth0, ok := v.Networks["eth0"]; ok {
				stat.networkReceivedBytes = eth0.RxBytes
				stat.networkTransmittedBytes = eth0.TxBytes
			} else if host, ok := v.Networks["host"]; ok {
				stat.networkReceivedBytes = host.RxBytes
				stat.networkTransmittedBytes = host.TxBytes
			} else {
				// Sum all networks if eth0/host not found
				for _, network := range v.Networks {
					stat.networkReceivedBytes += network.RxBytes
					stat.networkTransmittedBytes += network.TxBytes
				}
			}
		}

		// Block IO
		if v.BlkioStats.IoServiceBytesRecursive != nil {
			for _, io := range v.BlkioStats.IoServiceBytesRecursive {
				switch strings.ToUpper(io.Op) {
				case "READ":
					stat.blockIoReadBytes += io.Value
				case "WRITE":
					stat.blockIoWrittenBytes += io.Value
				}
			}
		}

		newStats[container.ID] = stat
	}

	c.mu.Lock()
	c.statsCache = newStats
	c.mu.Unlock()
}

type loggerWrapper struct {
	Logger *log.Logger
}

func (l *loggerWrapper) Println(v ...interface{}) {
	(*l.Logger).Log("message", fmt.Sprint(v...))
}

// Define loggers.
var (
	normalLogger = log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	errorLogger  = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
)

// parseCustomLabels parses CUSTOM_LABELS environment variable
// Format: "label1:container.label1,label2:container.label2"
func parseCustomLabels() map[string]string {
	customLabels := make(map[string]string)
	customLabelsEnv := os.Getenv("CUSTOM_LABELS")
	if customLabelsEnv == "" {
		return customLabels
	}
	
	// Split by comma to get individual label mappings
	parts := strings.Split(customLabelsEnv, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Split by colon to get final label name and container label key
		labelParts := strings.SplitN(part, ":", 2)
		if len(labelParts) == 2 {
			finalLabelName := strings.TrimSpace(labelParts[0])
			containerLabelKey := strings.TrimSpace(labelParts[1])
			if finalLabelName != "" && containerLabelKey != "" {
				customLabels[finalLabelName] = containerLabelKey
			}
		}
	}
	return customLabels
}

// Define flags.
var (
	address          = flag.String("listen-address", ":9100", "The address to listen on for HTTP requests.")
	port             = flag.Int("port", 0, "Port to listen on (overrides listen-address port)")
	interval         = flag.Int("interval", 15, "Interval in seconds to gather stats (minimum 3)")
	hostip           = flag.String("hostip", "", "Docker host IP (for TCP connection)")
	hostport         = flag.Int("hostport", 0, "Docker host port (for TCP connection)")
	collectdefault   = flag.Bool("collectdefault", false, "Collect default Prometheus metrics")
)

func init() {
	normalLogger = log.With(normalLogger, "timestamp", log.DefaultTimestampUTC)
	normalLogger = log.With(normalLogger, "severity", "info")
	errorLogger = log.With(errorLogger, "timestamp", log.DefaultTimestampUTC)
	errorLogger = log.With(errorLogger, "severity", "error")
	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
}

func main() {
	flag.Parse()

	// Override port if specified
	if *port > 0 {
		*address = fmt.Sprintf(":%d", *port)
	}

	// Ensure minimum interval
	if *interval < 3 {
		*interval = 3
	}

	// Validate configuration
	if *hostip != "" && *hostport <= 0 {
		errorLogger.Log("message", "hostip specified but hostport is missing or invalid")
		os.Exit(1)
	}
	if *hostip == "" && *hostport > 0 {
		errorLogger.Log("message", "hostport specified but hostip is missing")
		os.Exit(1)
	}

	// Create Docker client
	var dockerClient *client.Client
	var err error

	if *hostip != "" && *hostport > 0 {
		host := fmt.Sprintf("tcp://%s:%d", *hostip, *hostport)
		normalLogger.Log("message", fmt.Sprintf("INFO: Connecting to Docker on %s...", host))
		dockerClient, err = client.NewClientWithOpts(
			client.WithHost(host),
			client.WithAPIVersionNegotiation(),
		)
	} else {
		normalLogger.Log("message", "INFO: Connecting to Docker on /var/run/docker.sock...")
		dockerClient, err = client.NewEnvClient()
	}
	if err != nil {
		errorLogger.Log("message", fmt.Sprintf("Failed to create Docker client: %v", err))
		os.Exit(1)
	}
	defer dockerClient.Close()

	// Validate Docker connection
	_, err = dockerClient.Ping(context.Background())
	if err != nil {
		errorLogger.Log("message", fmt.Sprintf("Failed to ping Docker daemon: %v", err))
		os.Exit(1)
	}

	normalLogger.Log("message", "INFO: Registering Prometheus metrics...")

	// Parse custom labels from environment variable
	customLabels := parseCustomLabels()
	if len(customLabels) > 0 {
		normalLogger.Log("message", fmt.Sprintf("INFO: Loaded %d custom labels from CUSTOM_LABELS", len(customLabels)))
	}

	// Register container collector
	containerCollector := &dockerContainerCollector{
		containerClient: dockerClient,
		statsCache:      make(map[string]*containerStats),
		errorLogger:     errorLogger,
		customLabels:    customLabels,
	}
	prometheus.MustRegister(containerCollector)

	// Collect default metrics if requested
	if *collectdefault {
		prometheus.MustRegister(prometheus.NewGoCollector())
		prometheus.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	}

	// Start gathering resource stats metrics periodically
	containerCollector.refreshContainerResourceStatsCache()
	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			containerCollector.refreshContainerResourceStatsCache()
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Support GET only", http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "<h1>Gitlab Runner Container Exporter</h1>")
	})

	http.HandleFunc("/-/healthy", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "up")
	})

	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{ErrorLog: &loggerWrapper{Logger: &errorLogger}, EnableOpenMetrics: true}))

	normalLogger.Log("message", fmt.Sprintf("INFO: Docker Stats exporter listening on port %s", *address))

	server := &http.Server{Addr: *address, Handler: nil, ReadTimeout: 20 * time.Second}

	go func() {
		err = server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			errorLogger.Log("message", fmt.Sprintf("HTTP server error: %v", err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, os.Interrupt)
	<-quit
	normalLogger.Log("message", "Server shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		errorLogger.Log("message", fmt.Sprintf("Failed to gracefully shutdown: %v", err))
	}
	normalLogger.Log("message", "Server shutdown")
}
