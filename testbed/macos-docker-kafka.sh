#!/bin/bash

export KAFKA_ADVERTISED_HOST_NAME=$(ifconfig | grep "inet " | grep -v "127.0.0.1" | awk '{print $2}')
docker-compose up --scale kafka=3