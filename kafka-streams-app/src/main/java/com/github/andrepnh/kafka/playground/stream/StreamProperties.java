package com.github.andrepnh.kafka.playground.stream;

import java.nio.file.Paths;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;

public class StreamProperties {
  public static Properties newDefaultStreamProperties(String appId) {
    var properties = new Properties();

    String stateDir = Paths.get(System.getProperty("user.dir"), "stream-state").toString();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonNodeSerde.class);
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonNodeSerde.class);
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    return properties;
  }
}
