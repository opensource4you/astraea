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

get_ipv4_address() {
  os="$(uname)"

  if [[ "$os" == "Darwin" ]]; then
    ifconfig | awk '/inet / && $2 != "127.0.0.1" { print $2; exit }'
    return 0
  fi

  if command -v ip &>/dev/null; then
    ip -o -4 address show | awk '!/127.0.0.1/ {gsub(/\/.*/, "", $4); print $4; exit}'
  elif command -v ifconfig &>/dev/null; then
    ifconfig | awk '/inet / && $2 != "127.0.0.1" { print $2; exit }'
  elif command -v networksetup &>/dev/null; then
    networksetup -getinfo Wi-Fi | awk '/IP address:/ { print $3; exit }'
  else
    echo "Error: No supported command found to fetch IP address." >&2
    return 1
  fi
}

declare -r USER=astraea
declare -r BUILD=${BUILD:-false}
declare -r RUN=${RUN:-true}
declare -r ADDRESS=$(get_ipv4_address)

# ===================================[functions]===================================

function getRandomPort() {
  echo $(($(($RANDOM%10000))+10000))
}


# we need to parse the controller port from pre-defined `VOTERS`
function generateControllerPort() {
  if [[ -n "$VOTERS" ]]; then
    port=""
    IFS=',' read -ra ADDR <<< "$VOTERS"
    for voter in "${ADDR[@]}"; do
      if [[ "$voter" == "$NODE_ID"* ]]; then
        port=${voter##*:}
        break
      fi
    done
    echo "$port"
  else
    echo "$(getRandomPort)"
  fi
}

# don't change the length as it is expected 16 bytes of a base64-encoded UUID
function randomString() {
  openssl rand -base64 16 | tr -dc 'a-zA-Z0-9' | head -c 22
}

function checkDocker() {
  if [[ "$(which docker)" == "" ]]; then
    echo "you have to install docker"
    exit 2
  fi
}

function checkNetwork() {
  if [[ "$ADDRESS" == "127.0.0.1" || "$ADDRESS" == "127.0.1.1" ]]; then
    echo "Either 127.0.0.1 or 127.0.1.1 can't be used in this script. Please check /etc/hosts"
    exit 2
  fi
}

function buildImageIfNeed() {
  local imageName="$1"
  if [[ "$(docker images -q "$imageName" 2>/dev/null)" == "" ]]; then
    local needToBuild="true"
    if [[ "$BUILD" == "false" ]]; then
      docker pull "$imageName" 2>/dev/null
      if [[ "$?" == "0" ]]; then
        needToBuild="false"
      else
        echo "Can't find $imageName from repo. Will build $imageName on the local"
      fi
    fi
    if [[ "$needToBuild" == "true" ]]; then
      generateDockerfile
      docker build --no-cache -t "$imageName" -f "$DOCKERFILE" "$DOCKER_FOLDER"
      if [[ "$?" != "0" ]]; then
        exit 2
      fi
    fi
  fi
}
