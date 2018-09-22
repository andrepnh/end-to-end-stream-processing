package com.github.andrepnh.kafka.playground.stream;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.streams.StreamsConfig;

public class StreamTestProperties {
  public static Properties newDefaultStreamProperties() {
    String caller = Arrays
        .asList(Thread.currentThread().getStackTrace())
        .get(2) // Skip first element (the getStackTrace method), and the second (this method)
        .getClassName();
    var properties = new Properties();
    var stateDir = Paths.get(System.getProperty("user.dir"), "stream-state", "test");

    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, caller + "_" + UUID.randomUUID().toString());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "whatever");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonNodeSerde.class);
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonNodeSerde.class);
    return properties;
  }
}
