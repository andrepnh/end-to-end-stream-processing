Periodically inserts inventory data into a postgres service. Each insertion will trigger stream processing, whether done by Kafka Streams or KSQL.

To execute, first ensure the postgres service is up:
```bash
$ cd ../testbed
$ export DOCKER_HOST_IP=ip
$ docker-compose up
```

A note about `DOCKER_HOST_IP`, as mentioned in [wurstmeister/kafka-docker](https://github.com/wurstmeister/kafka-docker) one cannot assign it to `localhost` or `127.0.0.1`, but you can use the ip you got from any network you're connected to.

After the services are up, on another terminal do:

```bash
$ ./gradlew clean build && java -jar build/libs/inventory-generator-1.0-SNAPSHOT-all.jar
```

The application will print usage info and immediately start inserting data.