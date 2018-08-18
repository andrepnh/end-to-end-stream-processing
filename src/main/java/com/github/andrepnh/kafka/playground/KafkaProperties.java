package com.github.andrepnh.kafka.playground;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.UUID;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;

public final class KafkaProperties {
  public static final int BROKERS = 3;
  public static final ImmutableIntList KAFKA_HOST_PORTS;
  public static final String KAFKA_HOST;
  public static final String BOOTSTRAP_SERVERS;
  private static final AdminClient ADMIN_CLIENT;

  static {
    checkState(
        !Strings.isNullOrEmpty(System.getenv("DOCKER_HOST_IP")),
        "The environment variable DOCKER_HOST_IP is mandatory");
    KAFKA_HOST = System.getenv("DOCKER_HOST_IP");
    KAFKA_HOST_PORTS = new PortInspector().inspect(BROKERS).toImmutable();
    BOOTSTRAP_SERVERS = KAFKA_HOST_PORTS.collect(port -> KAFKA_HOST + ":" + port).makeString(",");
    ADMIN_CLIENT = AdminClient.create(
        ImmutableMap.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));
  }

  public static ImmutableMap.Builder<String, Object> newDefaultProducerProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BOOTSTRAP_SERVERS)
        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        .put(ProducerConfig.RETRIES_CONFIG, 5);
  }

  public static ImmutableMap.Builder<String, Object> newDefaultConsumerProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BOOTSTRAP_SERVERS)
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

  public static CreateTopicsResult createTopic(String name, int partitions, int replicationFactor) {
    return ADMIN_CLIENT.createTopics(Lists.newArrayList(
        new NewTopic(name, partitions, (short) replicationFactor)));
  }

  public static String uniqueTopic(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  private KafkaProperties() {}
}
