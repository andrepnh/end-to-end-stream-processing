#!/bin/bash

set -e

find config.d -maxdepth 1 -type f -exec curl -f -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d @{} http://$DOCKER_HOST_IP:8083/connectors \;