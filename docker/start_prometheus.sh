#!/bin/bash

# =============================[functions]=============================

function showHelp() {
  echo "Usage: start_prometheus.sh [ COMMAND ]"
  echo "COMMAND: "
  echo "    start <kafka-broker-jmx-addresses> <node-exporter-addresses>    create/start the prometheus docker instance"
  echo "    refresh <kafka-broker-jmx-addresses> <node-exporter-addresses>  refresh and apply the prometheus config"
  echo "    help                                                            show this dialog"
}

# ===============================[checks]===============================

if [[ "$(which docker)" == "" ]]; then
  echo "you have to install docker"
  exit 2
fi

if [[ "$(which ipconfig 2> /dev/null)" != "" ]]; then
  address=$(ipconfig getifaddr en0)
else
  address=$(hostname -i)
fi

if [[ "$address" == "127.0.0.1" || "$address" == "127.0.1.1" ]]; then
  echo "the address: Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
  exit 2
fi

# =================================[main]=================================

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

prometheus_port="${PROMETHEUS_PORT:-9090}"
image_name=prom/prometheus:v2.32.1
container_name="prometheus-${prometheus_port}"
file=/tmp/prometheus-${prometheus_port}.yml
scrape_interval="5s"
volume_name_1="prometheus-${prometheus_port}-etc"
volume_name_2="prometheus-${prometheus_port}-prometheus"

function write_config() {
  kafka_jmx_addresses="$(wrap_address "$1")"
  node_exporter_addresses="$(wrap_address "$2")"

  cat <<EOT > "$file"
global:
  scrape_interval: 15s
  external_labels:
    monitor: 'prometheus'

scrape_configs:

  - job_name: 'kafka'
    scrape_interval: $scrape_interval
    static_configs:
      - targets: [$kafka_jmx_addresses]

  - job_name: 'node'
    scrape_interval: $scrape_interval
    static_configs:
      - targets: [$node_exporter_addresses]
EOT
}

function is_prometheus_exists() {
  docker ps -a | grep "$container_name" > /dev/null
  if [ $? -eq 0 ]; then echo "yes"; else echo "no"; fi
}

function is_prometheus_running() {
  docker ps | grep "$container_name" > /dev/null
  if [ $? -eq 0 ]; then echo "yes"; else echo "no"; fi
}

function info() {
  # shellcheck disable=SC2068
  echo "[INFO]" $@
}

function refresh_config_from_file() {
  info "Refresh prometheus configuration from config file $file"
  docker cp "$file" "$container_name:/etc/prometheus/prometheus.yml"
  docker kill --signal="SIGHUP" "$container_name"
}

function refresh_config() {
  info "Refresh prometheus configuration"

  write_config "$1" "$2"
  docker cp "$file" "$container_name:/etc/prometheus/prometheus.yml"
  docker kill --signal="SIGHUP" "$container_name"
}

function main() {
  if [ "$(is_prometheus_running)" == "yes" ]; then
    info "Prometheus is already running" at "http://$address:$prometheus_port"
    showHelp
    exit 0
  fi

  if [ "$(is_prometheus_exists)" == "yes" ]; then
    info "Start existing prometheus instance"
    docker start "$container_name"
  else
    info "Create a new prometheus docker instance"
    docker run -d \
        --name "$container_name" \
        -p "$prometheus_port:9090" \
        -v "${volume_name_1}:/etc/prometheus" \
        -v "${volume_name_2}:/prometheus" \
        "$image_name"
  fi

  refresh_config "$1" "$2"

  info "================================================="
  info "config file: $file"
  info "prometheus address: http://${address}:$prometheus_port"
  info "command to run grafana at this host: ./docker/start_grafana.sh start"
  info "command to add prometheus to grafana datasource: ./docker/start_grafana.sh add_prom_source <USERNAME>:<PASSWORD> Prometheus http://$address:$prometheus_port"
  info "================================================="
}


# shellcheck disable=SC2199
if [[ "$1" == "start" ]]; then
  # shellcheck disable=SC2068
  main "$2" "$3"
elif [[ "$1" == "refresh" ]] && [[ "$2" != "" ]]; then
  # shellcheck disable=SC2068
  refresh_config "$2" "$3"
elif [[ "$1" == "refresh" ]]; then
  refresh_config_from_file
elif [[ "$1" == "help" ]]; then
  showHelp
else
  info Unknown argument: "$@"
  showHelp
fi