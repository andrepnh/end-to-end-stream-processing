package com.github.andrepnh.kafka.playground;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;

public final class ClusterProperties {
  public static final int BROKERS = Integer.parseInt(System.getenv("KAFKA_BROKERS"));
  public static final String BOOTSTRAP_SERVERS;

  static {
    checkState(
        !Strings.isNullOrEmpty(System.getenv("DOCKER_HOST_IP")),
        "The environment variable DOCKER_HOST_IP is mandatory");
    String kafkaHost = System.getenv("DOCKER_HOST_IP");
    ImmutableIntList kafkaPorts = new PortInspector().inspect(BROKERS).toImmutable();
    BOOTSTRAP_SERVERS = kafkaPorts.collect(port -> kafkaHost + ":" + port).makeString(",");
  }

  public static ImmutableMap.Builder<String, Object> newDefaultConsumerProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterProperties.BOOTSTRAP_SERVERS)
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

  private ClusterProperties() {}
}
