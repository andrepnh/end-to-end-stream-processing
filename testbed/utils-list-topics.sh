#!/bin/bash
docker-compose exec kafka /opt/kafka/bin/kafka-topics.sh --zookeeper zookeeper:2181 --list
