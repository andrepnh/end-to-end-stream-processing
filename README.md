Steps:

```bash
$ export DOCKER_HOST_IP=192.168.0.254
$ export KAFKA_BROKERS=1
$ ./gradlew clean build
$ docker inspect --format='{{(index (index .NetworkSettings.Ports "9092/tcp") 0).HostPort}}' testbed_kafka_1
$ java -Dkafka-ports=<result from previous line> -jar build/libs/kafka-playground-1.0-SNAPSHOT-all.jar
```

Need to clear state stores