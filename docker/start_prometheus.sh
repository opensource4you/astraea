#!/bin/bash

# =============================[functions]=============================

function showHelp() {
  echo "Usage: [targets]"
}

# ===============================[checks]===============================

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

if [[ "$(which ipconfig)" != "" ]]; then
  address=$(ipconfig getifaddr en0)
else
  address=$(hostname -i)
fi

if [[ "$address" == "127.0.0.1" || "$address" == "127.0.1.1" ]]; then
  echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
  exit 2
fi

# =================================[main]=================================

if [[ "$1" == "" ]] || [[ "$1" == "help" ]]; then
  showHelp
  exit 0
fi

targets=""
IFS=',' read -ra ADDR <<< "$1"
for i in "${ADDR[@]}"; do
  if [[ "$targets" == "" ]]; then
    targets="'$i'"
  else
    targets="$targets,'$i'"
  fi
done
echo "$targets"

image_name=prom/prometheus
prometheus_port="$(($(($RANDOM % 10000)) + 10000))"
file=/tmp/prometheus-${prometheus_port}.yml

cat <<EOT > "$file"
global:
  scrape_interval: 15s
  external_labels:
    monitor: 'kafka-monitor'

scrape_configs:
  - job_name: 'brokers'
    scrape_interval: 5s
    static_configs:
      - targets: [$targets]

EOT

docker run -d \
    -p $prometheus_port:9090 \
    -v $file:/etc/prometheus/prometheus.yml \
    prom/prometheus

echo "================================================="
echo "config file: $file"
echo "prometheus address: http://${address}:$prometheus_port"
echo "================================================="