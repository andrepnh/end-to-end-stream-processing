package com.github.andrepnh.kafka.playground.stream;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.andrepnh.kafka.playground.db.gen.StockState;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamProcessorTest {
  private TopologyTestDriver driver;

  private String stateDir;

  @Before
  public void setup() {
    Properties properties = StreamTestProperties.newDefaultStreamProperties();
    stateDir = properties.getProperty(StreamsConfig.STATE_DIR_CONFIG);
    driver = new TopologyTestDriver(
        new StreamProcessor().buildTopology(),
        properties);
  }

  @After
  public void teardown() {
    driver.close();
  }

  @Test
  public void shouldContinuouslyUpdateWarehouseCapacity() {
    final Warehouse warehouse1 = new Warehouse(1, "one", 100, 50, 50),
        warehouse2 = new Warehouse(2, "two", 200, 25 ,-25);

    pipe(stockItem(warehouse1, 1, 0, 10));
    pipe(warehouse1);
    ProducerRecord<WarehouseKey, Allocation> record =
        read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals(0.1, record.value().getHardAllocation(), 0.000001);
    assertEquals(0.0, record.value().getSoftAllocation(), 0.000001);
    pipe(stockItem(warehouse1, 1, 10, 0));
    pipe(warehouse1); // Piping again the same value to trigger joins
    record = read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals(0.1, record.value().getHardAllocation(), 0.000001);
    assertEquals(0.1, record.value().getSoftAllocation(), 0.000001);

    pipe(stockItem(warehouse2, 5, 100, 100));
    pipe(warehouse2);
    record = read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals(1, record.value().getHardAllocation(), 0.000001);
    assertEquals(0.5, record.value().getSoftAllocation(), 0.000001);
    pipe(stockItem(warehouse2, 333, 50, 50));
    pipe(warehouse2);
    record = read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals(0.5, record.value().getHardAllocation(), 0.000001);
    assertEquals(0.25, record.value().getSoftAllocation(), 0.000001);
  }

  @Test
  public void shouldProcessDbChangelogIntoWarehouseTopic() {
    var factory = new ConsumerRecordFactory<>(
        "connect_test.public.stockstate",
        new JsonNodeSerde().serializer(),
        new JsonNodeSerde().serializer());
    var state = new StockState(1, 3, 10, 5, 2,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    ImmutableList<KeyValue<JsonNode, JsonNode>> keyValues = new DebeziumJsonBuilder()
        .add(state)
        .build();
    driver.pipeInput(factory.create(keyValues));

    ProducerRecord<List<Integer>, StockQuantity> record = driver
        .readOutput("warehouse-stock",
            JsonSerde.of(new TypeReference<List<Integer>>() { }).deserializer(),
            JsonSerde.of(StockQuantity.class).deserializer());
    assertEqualsToRecord(state, record);
  }

  @Test
  public void shouldUpdateWarehouseTopicWithNewerDbUpdates() {
    var factory = new ConsumerRecordFactory<>(
        "connect_test.public.stockstate",
        new JsonNodeSerde().serializer(),
        new JsonNodeSerde().serializer());
    var initial = new StockState(1, 1, 10, 0, 0,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    var update = new StockState(1, 1, 10, 5, 0,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(3));
    ImmutableList<KeyValue<JsonNode, JsonNode>> keyValues = new DebeziumJsonBuilder()
        .add(initial)
        .add(update)
        .build();
    driver.pipeInput(factory.create(keyValues));

    ProducerRecord<List<Integer>, StockQuantity> record = readLast("warehouse-stock",
        new TypeReference<List<Integer>>() { }, StockQuantity.class);
    assertEqualsToRecord(update, record);
  }

  @Test
  public void shouldUpdateWarehouseTopicWithNewerDbUpdatesOnlyIfTheyAreReallyNew() {
    var update = new StockState(1, 1, 10, 0, 0,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    pipe(update);
    var lateUpdate = new StockState(1, 1, 10, 5, 0,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(10));
    pipe(lateUpdate);

    ProducerRecord<List<Integer>, StockQuantity> record = readLast("warehouse-stock",
        new TypeReference<List<Integer>>() { }, StockQuantity.class);
    assertEqualsToRecord(update, record);
  }

  @Test
  public void shouldUpdateWarehouseTopicWithNewWarehouses() {
    var warehouse1 = new StockState(1, 1, 10, 0, 0,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(10));
    var warehouse2 = new StockState(2, 1, 5, 5, 0,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    var warehouse3 = new StockState(3, 1, 3, 0, 2,
        ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(3));
    pipe(warehouse1, warehouse2, warehouse3);

    List<ProducerRecord<List<Integer>, StockQuantity>> records = readAll("warehouse-stock",
        new TypeReference<List<Integer>>() {}, StockQuantity.class);

    assertEqualsToRecord(Lists.newArrayList(warehouse1, warehouse2, warehouse3),
        records);
  }

  @Test
  public void shouldAggregateStockFromAllWarehousesToGlobal() {
    var stock1Warehouse1 = new StockState(1, 1, 10, 0, 0,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(10));
    var stock1Warehouse2 = new StockState(2, 1, 3, 1, 1,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    var stock2Warehouse2 = new StockState(2, 2, 7, 3, 2,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(17));
    var stock2Warehouse3 = new StockState(3, 2, 1, 1, 1,
        ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(5));
    pipe(stock1Warehouse1, stock1Warehouse2, stock2Warehouse2, stock2Warehouse3);

    List<ProducerRecord<Integer, Integer>> records = readAll(
        "global-stock", Serdes.Integer(), Serdes.Integer());

    Map<Integer, Integer> expectedHardQuantityByStockItem = Lists
        .newArrayList(stock1Warehouse1, stock1Warehouse2, stock2Warehouse2, stock2Warehouse3)
        .stream()
        .collect(Collectors.groupingBy(
            StockState::getStockItemId,
            Collectors.summingInt(state -> StockQuantity.of(state).hardQuantity())));

    Map<Integer, Integer> hardQuantityByStockItem = records.stream()
        .collect(Collectors.groupingBy(
            ProducerRecord::key,
            Collectors.reducing(0, ProducerRecord::value, (acc, curr) -> curr)));

    assertEquals(expectedHardQuantityByStockItem, hardQuantityByStockItem);
  }

  private <K, V> ProducerRecord<K, V> readLast(String topic,
      TypeReference<K> keyType, Class<V> valueType) {
    return readStream(topic, JsonSerde.of(keyType), JsonSerde.of(valueType))
        .sequential()
        .reduce((acc, current) -> current)
        .orElse(null);
  }

  private <K, V> ProducerRecord<K, V> readLast(String topic,
      Serde<K> keySerde, Serde<V> valueSerde) {
    return readStream(topic, keySerde, valueSerde)
        .sequential()
        .reduce((acc, current) -> current)
        .orElse(null);
  }

  private <K, V> List<ProducerRecord<K, V>> readAll(String topic, TypeReference<K> keyType,
      Class<V> valueType) {
    return readStream(topic, JsonSerde.of(keyType), JsonSerde.of(valueType))
        .collect(Collectors.toList());
  }

  private <K, V> List<ProducerRecord<K, V>> readAll(
      String topic, Serde<K> keySerde, Serde<V> valueSerde) {
    return readStream(topic, keySerde, valueSerde).collect(Collectors.toList());
  }

  private <K, V> Stream<ProducerRecord<K, V>> readStream(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
    return Stream
        .generate(() -> read(topic, keySerde, valueSerde))
        .takeWhile(Objects::nonNull);
  }

  private <K, V> ProducerRecord<K, V> read(String topic, Serde<K> keySerde, Serde<V> valueSerde) {
    return driver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer());
  }

  private void assertEqualsToRecord(List<StockState> states,
      List<ProducerRecord<List<Integer>, StockQuantity>> records) {
    assertEquals(states.size(), records.size());
    states.sort(Comparator
        .comparingInt(StockState::getWarehouseId)
        .thenComparing(StockState::getStockItemId));
    records.sort(Comparator
        .<ProducerRecord<List<Integer>, StockQuantity>>comparingInt(record -> record.key().get(0))
        .thenComparing(record -> record.key().get(1)));
    Streams.zip(states.stream(), records.stream(), Tuples::pair)
        .forEach(pair -> assertEqualsToRecord(pair.getOne(), pair.getTwo()));
  }

  private void assertEqualsToRecord(StockState state,
      ProducerRecord<List<Integer>, StockQuantity> record) {
    var warehouseItemIdsPair = record.key();
    assertEquals(state.getWarehouseId(), (int) warehouseItemIdsPair.get(0));
    assertEquals(state.getStockItemId(), (int) warehouseItemIdsPair.get(1));
    var stockQuantity = record.value();
    assertEquals(state.getSupply(), stockQuantity.getSupply());
    assertEquals(state.getDemand(), stockQuantity.getDemand());
    assertEquals(state.getReserved(), stockQuantity.getReserved());
    assertEquals(state.getLastUpdate(), stockQuantity.getLastUpdate());
  }

  private void pipe(Warehouse first, Warehouse... rest) {
    var builder = new DebeziumJsonBuilder();
    Lists.asList(first, rest).forEach(builder::add);
    pipe("connect_test.public.warehouse", builder.build());
  }

  private void pipe(StockState first, StockState... rest) {
    var builder = new DebeziumJsonBuilder();
    Lists.asList(first, rest).forEach(builder::add);
    pipe("connect_test.public.stockstate", builder.build());
  }

  private void pipe(String topic, List<KeyValue<JsonNode, JsonNode>> records) {
    var factory = new ConsumerRecordFactory<>(topic,
        new JsonNodeSerde().serializer(),
        new JsonNodeSerde().serializer());
    driver.pipeInput(factory.create(records));
  }

  private StockState stockItem(Warehouse warehouse, int id, int demand, int reserved) {
    return new StockState(warehouse.getId(),
        id,
        100000, // Doesn't matter
        demand,
        reserved,
        ZonedDateTime.now(ZoneOffset.UTC));
  }
}
