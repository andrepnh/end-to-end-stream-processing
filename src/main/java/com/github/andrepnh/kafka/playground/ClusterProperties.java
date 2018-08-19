package com.github.andrepnh.kafka.playground;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.UUID;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;

public final class ClusterProperties {
  public static final int BROKERS = 3;
  public static final ImmutableIntList KAFKA_HOST_PORTS;
  public static final String KAFKA_HOST;
  public static final String BOOTSTRAP_SERVERS;

  static {
    checkState(
        !Strings.isNullOrEmpty(System.getenv("DOCKER_HOST_IP")),
        "The environment variable DOCKER_HOST_IP is mandatory");
    KAFKA_HOST = System.getenv("DOCKER_HOST_IP");
    KAFKA_HOST_PORTS = new PortInspector().inspect(BROKERS).toImmutable();
    BOOTSTRAP_SERVERS = KAFKA_HOST_PORTS.collect(port -> KAFKA_HOST + ":" + port).makeString(",");
  }



  public static ImmutableMap.Builder<String, Object> newDefaultConsumerProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterProperties.BOOTSTRAP_SERVERS)
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

  private ClusterProperties() {}
}
