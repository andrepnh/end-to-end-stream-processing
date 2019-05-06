package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.andrepnh.kafka.playground.Main;
import com.github.andrepnh.kafka.playground.db.StockQuantity;
import com.github.andrepnh.kafka.playground.db.Warehouse;
import com.google.common.collect.Lists;
import com.google.common.collect.MoreCollectors;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;

public class BaseStreamTest {
  protected TopologyTestDriver driver;

  @Before
  public void setup() {
    Properties properties = StreamTestProperties.newDefaultStreamProperties();
    driver = new TopologyTestDriver(
        new Main().buildTopology(),
        properties);
  }

  @After
  public void teardown() {
    driver.close();
  }

  protected <K, V> ProducerRecord<K, V> readLast(String topic,
      TypeReference<K> keyType, Class<V> valueType) {
    return readStream(topic, JsonSerde.of(keyType), JsonSerde.of(valueType))
        .sequential()
        .reduce((acc, current) -> current)
        .orElse(null);
  }

  protected <K, V> ProducerRecord<K, V> readLast(String topic,
      Serde<K> keySerde, Serde<V> valueSerde) {
    return readStream(topic, keySerde, valueSerde)
        .sequential()
        .reduce((acc, current) -> current)
        .orElse(null);
  }

  protected <K, V> ProducerRecord<K, V> readSingle(String topic,
      Serde<K> keySerde, Serde<V> valueSerde) {
    return readStream(topic, keySerde, valueSerde)
        .sequential()
        .collect(MoreCollectors.onlyElement());
  }

  protected <K, V> List<ProducerRecord<K, V>> readAll(String topic, TypeReference<K> keyType,
      Class<V> valueType) {
    return readStream(topic, JsonSerde.of(keyType), JsonSerde.of(valueType))
        .collect(Collectors.toList());
  }

  protected <K, V> List<ProducerRecord<K, V>> readAll(
      String topic, Serde<K> keySerde, Serde<V> valueSerde) {
    return readStream(topic, keySerde, valueSerde).collect(Collectors.toList());
  }

  protected <K, V> Stream<ProducerRecord<K, V>> readStream(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
    return Stream
        .generate(() -> read(topic, keySerde, valueSerde))
        .takeWhile(Objects::nonNull);
  }

  protected <K, V> ProducerRecord<K, V> read(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
    return driver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer());
  }

  protected void pipe(Warehouse first, Warehouse... rest) {
    var builder = new DebeziumJsonBuilder();
    Lists.asList(first, rest).forEach(builder::add);
    pipe("connect_test.public.warehouse", builder.build());
  }

  protected void pipe(StockQuantity first, StockQuantity... rest) {
    var builder = new DebeziumJsonBuilder();
    Lists.asList(first, rest).forEach(builder::add);
    pipe("connect_test.public.stockquantity", builder.build());
  }

  protected void pipe(String topic, List<KeyValue<JsonNode, JsonNode>> records) {
    var factory = new ConsumerRecordFactory<>(topic,
        new JsonNodeSerde().serializer(),
        new JsonNodeSerde().serializer());
    driver.pipeInput(factory.create(records));
  }

  protected StockQuantity stockQuantity(Warehouse warehouse, int id, int quantity) {
    return new StockQuantity(warehouse.getId(),
        id,
        quantity,
        ZonedDateTime.now(ZoneOffset.UTC));
  }
}
