
# Gitlab Runner Container Exporter

This is an exporter for docker-in-docker gitlab runners, where jobs are run as docker containers.

The code is a combo of two other exporters:
- [karugaru/docker_state_exporter](https://github.com/karugaru/docker_state_exporter) 
- [wywywywy/docker_stats_exporter](https://github.com/wywywywy/docker_stats_exporter)

I merged them to gather two useful sets of metrics in one. To make it better suited for runners, I have also added some runner-specific labels of my own.

## Installation and Usage

The `Gitlab Runner Container Exporter` listens on HTTP port 9100 by default.

### Docker

For Docker run.

```bash
sudo docker run -d \
  -v "/var/run/docker.sock:/var/run/docker.sock" \
  -p 9100:9100 \
  evakdev/gitlab_runner_exporter \
  -listen-address=:8080
```

For Docker compose.

```yaml
---
version: '3.8'

services:
  gitlab_runner_exporter:
    image: evakdev/gitlab_runner_exporter
    volumes:
      - type: bind
        source: /var/run/docker.sock
        target: /var/run/docker.sock
    ports:
      - "8080:8080"
```

## Metrics

This exporter will export the following metrics:

- gitlab_runner_container_info
- gitlab_runner_container_health_status
- gitlab_runner_container_status
- gitlab_runner_container_startedat
- gitlab_runner_container_finishedat
- gitlab_runner_container_restart_count
- gitlab_runner_container_oomkilled
- gitlab_runner_container_memory_usage_ratio
- gitlab_runner_container_memory_usage_bytes
- gitlab_runner_container_memory_usage_rss_bytes
- gitlab_runner_container_memory_limit_bytes
- gitlab_runner_container_cpu_usage_ratio
- gitlab_runner_container_blockio_read_bytes
- gitlab_runner_container_blockio_written_bytes
- gitlab_runner_container_network_received_bytes
- gitlab_runner_container_network_transmitted_bytes

The source of these metrics are `docker inspect` for health and status, and `docker stats` for resource-related metrics. (Except for oomkilled which comes from docker inspect).

This exporter also exports the standard
[Go Collector](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#NewGoCollector)
and [Process Collector](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#NewProcessCollector).


## Labels
All metrics have `id` (shortened) and `name` labels by default.


`gitlab_runner_container_info` will also include `image`, plus the following labels for gitlab runner job containers:

- job_id
- job_ref
- job_commit_sha (shortened)
- job_url_id
- job_pipeline_id
- job_project_id
- job_runner_id

You can also add your own labels to `gitlab_runner_container_info` by setting `CUSTOM_LABELS` environment variable:
```
CUSTOM_LABELS="final_label_1:raw_label_1,final_label_2:raw_label_2"
```


## Caution

This exporter will do a docker inspect every time prometheus pulls.\
If a large number of requests are made, there will be performance issues. (I think. Not verified.)\
So, this app caches the result of docker inspect for 1 second.
So, please note that if you set the scrape_interval of prometheus to less than one second, you may get the same result back.

### Build

```bash
git clone https://github.com/evakdev/gitlab_runner_exporter
cd gitlab_runner_exporter
sudo docker build -t gitlab_runner_exporter_test .
```

### Run

```bash
sudo docker run -d \
  -v "/var/run/docker.sock:/var/run/docker.sock" \
  -p 8080:8080 \
  gitlab_runner_exporter_test \
  -listen-address=:8080
```
