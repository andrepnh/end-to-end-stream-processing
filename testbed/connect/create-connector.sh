#!/bin/bash

curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d @connector-config.json http://$DOCKER_HOST_IP:8083/connectors