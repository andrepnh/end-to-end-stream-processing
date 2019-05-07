Periodically inserts inventory data into a postgres service. Each insertion will trigger stream processing, whether done by Kafka Streams or KSQL.

To execute, first ensure the postgres service is up:
```bash
$ cd ../testbed
$ export DOCKER_HOST_IP=ip
$ docker-compose up -d postgres
```

After the service is up, do:

```bash
$ docker-compose up generator
```

Data generation can be configured with the following system properties:

1. `max.warehouses`:, the maximum amount of warehouses, defaults to 300.
2. `max.items`: the maximum mount of products, defaults to 500.
3. `milliseconds.sleep`: the amount of time each thread will sleep after generating a stock entry, defaults to 400.
4. `stock.to.generate`: stocks to create, the application will finish once this number is reached. Default is 100000.

Warehouses and items are created as needed and the first two properties only dictates the maximum available. Due to randomness the actual amount could be lower.