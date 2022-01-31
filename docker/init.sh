#!/bin/bash
declare -r USER=astraea
declare -r IMAGE_NAME="$REPO:$VERSION"
declare -r BUILD=${BUILD:-false}
declare -r RUN=${RUN:-true}

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


