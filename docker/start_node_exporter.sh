#!/bin/bash

# node_exporter run at 9100 port by default
# there is no reason to run two node_exporters at same box
PORT="${PORT:-9100}"

function error() {
  echo "[ERROR]" "$@"
}

function info() {
  echo "[INFO]" "$@"
}


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

if [[ "$(curl --connect-timeout 1 -s "http://0.0.0.0:$PORT")" != "" ]]; then
  error Is address http://0.0.0.0:"$PORT" already in use by other application?
  exit 4
fi


function docker_container_running() {
  # return true if service alive, or false if service is not running
  docker container inspect -f '{{.State.Running}}' "$1"
}


# =================================[main]=================================

# let node_exporter listen at 0.0.0.0, so anyone can access it from localhost or hostname
function run_node_exporter() {
  docker run -d \
    --net="host" \
    --pid="host" \
    -v "/:/host:ro,rslave" \
    prom/node-exporter \
    --path.rootfs=/host \
    --web.listen-address="0.0.0.0:$PORT"
}
container_id="$(run_node_exporter)"
info Container ID of node_exporter: "$container_id"

if [[ "$(curl --connect-timeout 1 --retry 3 -s "http://0.0.0.0:$PORT")" == "" ]] || [[ "$(docker_container_running "$container_id")" == "false" ]]; then
  error Cannot access node_exporter metric server: "http://0.0.0.0:$PORT". Something go wrong!
  error Execute \"docker logs "$container_id"\" to see what\'s going on.
  exit 3
fi

info node_exporter running at "http://$address:$PORT"