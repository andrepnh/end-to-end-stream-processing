package com.github.andrepnh.kafka.playground.stream;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.Test;

public class StreamProcessorTest extends BaseStreamTest {

  @Test
  public void shouldProcessDbChangelogIntoWarehouseTopic() {
    var factory = new ConsumerRecordFactory<>(
        "connect_test.public.stockstate",
        new JsonNodeSerde().serializer(),
        new JsonNodeSerde().serializer());
    var state = new StockQuantity(1, 3, 10,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    ImmutableList<KeyValue<JsonNode, JsonNode>> keyValues = new DebeziumJsonBuilder()
        .add(state)
        .build();
    driver.pipeInput(factory.create(keyValues));

    ProducerRecord<List<Integer>, Quantity> record = driver
        .readOutput("warehouse-stock",
            JsonSerde.of(new TypeReference<List<Integer>>() { }).deserializer(),
            JsonSerde.of(Quantity.class).deserializer());
    assertEqualsToRecord(state, record);
  }

  @Test
  public void shouldUpdateWarehouseTopicWithNewerDbUpdates() {
    var factory = new ConsumerRecordFactory<>(
        "connect_test.public.stockstate",
        new JsonNodeSerde().serializer(),
        new JsonNodeSerde().serializer());
    var initial = new StockQuantity(1, 1, 10,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    var update = new StockQuantity(1, 1, 20,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(3));
    ImmutableList<KeyValue<JsonNode, JsonNode>> keyValues = new DebeziumJsonBuilder()
        .add(initial)
        .add(update)
        .build();
    driver.pipeInput(factory.create(keyValues));

    ProducerRecord<List<Integer>, Quantity> record = readLast("warehouse-stock",
        new TypeReference<List<Integer>>() { }, Quantity.class);
    assertEqualsToRecord(update, record);
  }

  @Test
  public void shouldUpdateWarehouseTopicWithNewerDbUpdatesOnlyIfTheyAreReallyNew() {
    var update = new StockQuantity(1, 1, 20,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    pipe(update);
    var lateUpdate = new StockQuantity(1, 1, 10,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(10));
    pipe(lateUpdate);

    ProducerRecord<List<Integer>, Quantity> record = readLast("warehouse-stock",
        new TypeReference<List<Integer>>() { }, Quantity.class);
    assertEqualsToRecord(update, record);
  }

  @Test
  public void shouldUpdateWarehouseTopicWithNewWarehouses() {
    var warehouse1 = new StockQuantity(1, 1, 10,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(10));
    var warehouse2 = new StockQuantity(2, 1, 5,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    var warehouse3 = new StockQuantity(3, 1, 3,
        ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(3));
    pipe(warehouse1, warehouse2, warehouse3);

    List<ProducerRecord<List<Integer>, Quantity>> records = readAll("warehouse-stock",
        new TypeReference<List<Integer>>() {}, Quantity.class);

    assertEqualsToRecord(Lists.newArrayList(warehouse1, warehouse2, warehouse3),
        records);
  }

  @Test
  public void shouldAggregateStockFromAllWarehousesToGlobal() {
    var stock1Warehouse1 = new StockQuantity(1, 1, 10,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(10));
    var stock1Warehouse2 = new StockQuantity(2, 1, 3,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(5));
    var stock2Warehouse2 = new StockQuantity(2, 2, 7,
        ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(17));
    var stock2Warehouse3 = new StockQuantity(3, 2, 1,
        ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(5));
    pipe(stock1Warehouse1, stock1Warehouse2, stock2Warehouse2, stock2Warehouse3);

    List<ProducerRecord<Integer, Integer>> records = readAll(
        "global-stock", Serdes.Integer(), Serdes.Integer());

    Map<Integer, Integer> expectedQuantityByStockItem = Lists
        .newArrayList(stock1Warehouse1, stock1Warehouse2, stock2Warehouse2, stock2Warehouse3)
        .stream()
        .collect(Collectors.groupingBy(
            StockQuantity::getStockItemId,
            Collectors.summingInt(state -> Quantity.of(state).getQuantity())));

    Map<Integer, Integer> quantityByStockItem = records.stream()
        .collect(Collectors.groupingBy(
            ProducerRecord::key,
            Collectors.reducing(0, ProducerRecord::value, (acc, curr) -> curr)));

    assertEquals(expectedQuantityByStockItem, quantityByStockItem);
  }

  private void assertEqualsToRecord(List<StockQuantity> states,
      List<ProducerRecord<List<Integer>, Quantity>> records) {
    assertEquals(states.size(), records.size());
    states.sort(Comparator
        .comparingInt(StockQuantity::getWarehouseId)
        .thenComparing(StockQuantity::getStockItemId));
    records.sort(Comparator
        .<ProducerRecord<List<Integer>, Quantity>>comparingInt(record -> record.key().get(0))
        .thenComparing(record -> record.key().get(1)));
    Streams.zip(states.stream(), records.stream(), Tuples::pair)
        .forEach(pair -> assertEqualsToRecord(pair.getOne(), pair.getTwo()));
  }

  private void assertEqualsToRecord(StockQuantity state,
      ProducerRecord<List<Integer>, Quantity> record) {
    var warehouseItemIdsPair = record.key();
    assertEquals(state.getWarehouseId(), (int) warehouseItemIdsPair.get(0));
    assertEquals(state.getStockItemId(), (int) warehouseItemIdsPair.get(1));
    var stockQuantity = record.value();
    assertEquals(state.getQuantity(), stockQuantity.getQuantity());
    assertEquals(state.getLastUpdate(), stockQuantity.getLastUpdate());
  }


}
