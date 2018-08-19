package com.github.andrepnh.kafka.playground;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public final class ProducerFacade {

  /**
   * Same as {@code produce(topic, records, newDefaultProducerProperties().build()}.
   * @see #produce(String, int, Map)
   * @see #newDefaultProducerProperties()
   */
  public static void produce(String topic, int records) {
    produce(topic, records, newDefaultProducerProperties().build());
  }

  /**
   * Create and send records with unique keys to a topic. Key uniqueness is not ensure if this method
   * is called more than once for the same topic.
   *
   * @param records how many records to create
   * @param properties producer properties
   */
  public static void produce(String topic, int records, Map<String, Object> properties) {
    try (var producer = new KafkaProducer<>(properties)) {
      for (int i = 0; i < records; i++) {
        producer.send(new ProducerRecord<>(topic, "key_" + i, "value_" + i));
      }
    }
  }

  public static ImmutableMap.Builder<String, Object> newDefaultProducerProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterProperties.BOOTSTRAP_SERVERS)
        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        .put(ProducerConfig.RETRIES_CONFIG, 5);
  }

  private ProducerFacade() { }
}
