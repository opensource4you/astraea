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

declare -r USER=astraea
declare -r BUILD=${BUILD:-false}
declare -r RUN=${RUN:-true}
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)

# ===================================[functions]===================================

function getRandomPort() {
  echo $(($(($RANDOM%10000))+10000))
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
      echo "local!!!!!!!"
      generateDockerfile
      docker build --no-cache -t "$imageName" -f "$DOCKERFILE" "$DOCKER_FOLDER"
      if [[ "$?" != "0" ]]; then
        exit 2
      fi
    fi
  fi
}
