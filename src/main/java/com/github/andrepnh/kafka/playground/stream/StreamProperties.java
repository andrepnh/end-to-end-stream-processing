package com.github.andrepnh.kafka.playground.stream;

import com.github.andrepnh.kafka.playground.ClusterProperties;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;

public class StreamProperties {
  public static Properties newDefaultStreamProperties(String appId) {
    var properties = new Properties();

    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterProperties.BOOTSTRAP_SERVERS);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonNodeSerde.class);
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonNodeSerde.class);
    properties.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("user.dir"));
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return properties;
  }
}
