The best way I could think of to quickly grasp Kafka's consumer and producer API, specially consumer groups, was to build executable tests to assert the properties I expected from reading the documentation. This is what this project is about.

To execute, do:
```bash
$ export DOCKER_HOST_IP=ip
$ ./gradlew clean build
```

A note about `DOCKER_HOST_IP`, as mentioned in [wurstmeister/kafka-docker](https://github.com/wurstmeister/kafka-docker) one cannot assign it to `localhost` or `127.0.0.1`, but you can use the ip you got from any network you're connected to.