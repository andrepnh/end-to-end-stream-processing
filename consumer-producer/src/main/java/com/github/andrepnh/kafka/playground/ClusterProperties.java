package com.github.andrepnh.kafka.playground;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Map.Entry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.primitive.IntLists;

public final class ClusterProperties {
  public static final int BROKERS;
  public static final ImmutableIntList KAFKA_HOST_PORTS;
  public static final String KAFKA_HOST;
  public static final String BOOTSTRAP_SERVERS;
  private static final String HOST_PORT_ENV_VAR_PATTERN = "KAFKA_\\d_TCP_\\d+";

  static {
    checkState(
        !Strings.isNullOrEmpty(System.getenv("DOCKER_HOST_IP")),
        "The environment variable DOCKER_HOST_IP is mandatory");
    KAFKA_HOST = System.getenv("DOCKER_HOST_IP");
    KAFKA_HOST_PORTS = Maps.immutable.ofMap(System.getenv())
        .select((key, value) -> key.matches(HOST_PORT_ENV_VAR_PATTERN))
        .valuesView()
        .collectInt(Integer::parseInt)
        .toList()
        .toImmutable();
    checkState(
        !KAFKA_HOST_PORTS.isEmpty(),
        "No kafka host ports found on env, make sure variables are available and named "
            + "using the pattern %s",
        HOST_PORT_ENV_VAR_PATTERN);
    BOOTSTRAP_SERVERS = KAFKA_HOST_PORTS.collect(port -> KAFKA_HOST + ":" + port).makeString(",");
    BROKERS = KAFKA_HOST_PORTS.size();
  }

  public static ImmutableMap.Builder<String, Object> newDefaultConsumerProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterProperties.BOOTSTRAP_SERVERS)
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

  private ClusterProperties() {}
}
