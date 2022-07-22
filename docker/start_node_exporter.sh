#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

if curl --connect-timeout 1 --retry 3 --retry-connrefused -s "http://$address:$PORT/metrics" > /dev/null; then
  info node_exporter running at "http://$address:$PORT"
else
  error Cannot access node_exporter metric server: "http://$address:$PORT/metrics". Something go wrong!
  error Execute \"docker logs "$container_id"\" to see what\'s going on.
  exit 3
fi
