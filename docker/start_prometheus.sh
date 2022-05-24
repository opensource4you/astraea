#!/bin/bash

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source $DOCKER_FOLDER/docker_build_common.sh
declare -r PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
declare -r IMAGE_NAME=prom/prometheus:v2.32.1
declare -r CONTAINER_NAME="prometheus-${PROMETHEUS_PORT}"
declare -r CONFIGURATION_FILE="/tmp/prometheus-${PROMETHEUS_PORT}.yml"
declare -r SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-1s}"
declare -r SCRAPE_TIMEOUT="${SCRAPE_TIMEOUT:-1s}"

# =============================[functions]=============================

function showHelp() {
  echo "Usage: start_prometheus.sh [ COMMAND ]"
  echo "COMMAND: "
  echo "    start <node-exporter-addresses>    create/start the prometheus docker instance"
  echo "    help                               show this dialog"
}

function wrap_address() {
  _value=""
  IFS=',' read -ra ADDR <<< "$1"
  for i in "${ADDR[@]}"; do
    if [[ "$_value" == "" ]]; then
      _value="'$i'"
    else
      _value="$_value,'$i'"
    fi
  done
  echo "$_value"
}

function write_config() {
  node_exporter_addresses="$(wrap_address "$1")"

  cat <<EOT > "$CONFIGURATION_FILE"
global:
  scrape_interval: 15s
  external_labels:
    monitor: 'prometheus'

scrape_configs:

  - job_name: 'node'
    scrape_interval: $SCRAPE_INTERVAL
    scrape_timeout: $SCRAPE_TIMEOUT
    static_configs:
      - targets: [$node_exporter_addresses]
EOT
}

function info() {
  # shellcheck disable=SC2068
  echo "[INFO]" $@
}

function main() {
  docker run -d \
      --name "$CONTAINER_NAME" \
      -p "$PROMETHEUS_PORT:9090" \
      -v "$CONFIGURATION_FILE:/etc/prometheus/prometheus.yml" \
      "$IMAGE_NAME"

  info "================================================="
  info "config file: $CONFIGURATION_FILE"
  info "prometheus address: http://${ADDRESS}:$PROMETHEUS_PORT"
  info "command to run grafana at this host: $DOCKER_FOLDER/start_grafana.sh start"
  info "command to add prometheus to grafana datasource: $DOCKER_FOLDER/start_grafana.sh add_prom_source <USERNAME>:<PASSWORD> Prometheus http://$ADDRESS:$PROMETHEUS_PORT"
  info "================================================="
}

# =================================[main]=================================

checkDocker
checkNetwork

# shellcheck disable=SC2199
if [[ "$1" == "start" ]]; then
  # shellcheck disable=SC2068
  write_config "$2"
  main
elif [[ "$1" == "help" ]]; then
  showHelp
else
  info Unknown argument: "$@"
  showHelp
fi
