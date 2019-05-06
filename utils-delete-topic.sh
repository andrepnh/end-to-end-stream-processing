#!/bin/bash

# Avoiding absolute bash path; the git bash used on docker for windows tries to maps the absolute path to a windows folder 
docker-compose exec kafka bash -c "/opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper:2181 --delete --topic $1"
