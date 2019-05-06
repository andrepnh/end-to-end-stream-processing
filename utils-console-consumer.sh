#!/bin/bash
 
docker-compose exec kafka bash -c "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \"$@\""
