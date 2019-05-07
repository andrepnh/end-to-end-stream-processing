graph TD
    G[Inventory generator]-->|inserts| P[Postgres]
    DC[Debezium connector]-->|capture changes| P
    DC-->|produces| K[Kafka]
    S[Kafka Streams app]-->|aggregates| K
    EC[Elasticsearch connector]-->|consumes| K
    EC --> |pushes| E[Elasticsearch]
    KI[Kibana] --> |visualizes| E