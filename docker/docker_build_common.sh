#!/bin/bash
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
      docker build --no-cache -t "$imageName" -f "$DOCKERFILE" "$DOCKER_FOLDER"
      if [[ "$?" != "0" ]]; then
        exit 2
      fi
    fi
  fi
}
