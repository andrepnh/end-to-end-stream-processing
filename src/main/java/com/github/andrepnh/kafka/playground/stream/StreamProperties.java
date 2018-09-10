package com.github.andrepnh.kafka.playground.stream;

import com.github.andrepnh.kafka.playground.ClusterProperties;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class StreamProperties {
  public static ImmutableMap.Builder<String, Object> newDefaultStreamProperties(String appId) {
    return ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterProperties.BOOTSTRAP_SERVERS)
        .put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
        .put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonNodeSerde.class)
        .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonNodeSerde.class)
        .put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("user.dir"));
  }
}
