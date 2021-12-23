#!/bin/bash

# =================================[help]=================================

function showHelp() {
  echo "Usage: grafana.sh [ COMMAND ]"
  echo "COMMAND: "
  echo "    start(default)                              create/start the Grafana docker instance"
  echo "    add_prom_source <user:passwd> <name> <url>  add new prometheus data source"
  echo "    stop                                        stop the Grafana docker instance"
  echo "    help                                        show this dialog"
  echo "EXAMPLE: add new prometheus datasource"
  echo "    ./grafana.sh add_prom_source MY_USERNAME:MY_PASSWORD Prometheus http://localhost:9090"
}

# =================================[utility]=================================

function random_port() {
  echo "$(($((RANDOM % 16384)) + 49152))"
}

function info() {
  echo "[INFO]" "$@"
}
function grafana_dashboard_http_url() {
  echo "http://${address}:${GRAFANA_HTTP_DASHBOARD_PORT}"
}

function quiet_grep() {
  grep "$@" > /dev/null
}

function is_grafana_running() {
  curl --connect-timeout 2 -s "$(grafana_dashboard_http_url)/login" | quiet_grep grafana
  if [ $? -eq 0 ]; then echo "yes"; else echo "no"; fi
}

function add_prometheus_datasource() {
  usernamePassword="$1"
  source_name="$2"
  url="$3"
  curl \
    --connect-timeout 2 \
    -X "POST" \
    -s "http://$address:$GRAFANA_HTTP_DASHBOARD_PORT/api/datasources" \
    -H "Content-Type: application/json" \
    -u "$usernamePassword" \
    --data "{ \"name\":\"$source_name\", \"type\":\"prometheus\", \"url\":\"$url\", \"access\":\"proxy\", \"basicAuth\":false }"
}

GRAFANA_HTTP_DASHBOARD_PORT="3000"
GRAFANA_CONTAINER_NAME="Grafana_$GRAFANA_HTTP_DASHBOARD_PORT"

# =================================[checks]=================================
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

function stop() {
  docker stop $GRAFANA_CONTAINER_NAME
}

function main() {
  if [[ "$(is_grafana_running)" == "yes" ]]; then
    info Grafana is already running at "$(grafana_dashboard_http_url)"
    showHelp
    exit 0
  fi

  if [[ "$(docker ps -a | grep "$GRAFANA_CONTAINER_NAME")" != "" ]]; then
    info Restart Grafana docker image
    docker start "$GRAFANA_CONTAINER_NAME"
  else
    docker run -d \
        --name "$GRAFANA_CONTAINER_NAME" \
        -p "$GRAFANA_HTTP_DASHBOARD_PORT":3000 \
        grafana/grafana-oss
    info "Default username/password for grafana docker image is admin/admin"
  fi

  info "Access Grafana dashboard here: " "$(grafana_dashboard_http_url)"
}

# shellcheck disable=SC2199
if [[ "$@" == "" || "$1" == "start" ]]; then
  main
elif [[ "$1" == "add_prom_source" ]]; then
  add_prometheus_datasource "$2" "$3" "$4"
elif [[ "$1" == "stop" ]]; then
  stop
elif [[ "$1" == "help" ]]; then
  showHelp
else
  info Unknown argument: "$@"
  showHelp
fi