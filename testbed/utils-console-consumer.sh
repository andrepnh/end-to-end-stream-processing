#!/bin/bash

# Avoiding absolute bash path; the git bash used on docker for windows tries to maps the absolute path to a windows folder 
docker-compose exec kafka bash -c "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \"$@\""