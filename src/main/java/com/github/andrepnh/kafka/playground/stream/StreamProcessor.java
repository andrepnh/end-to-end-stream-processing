package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class StreamProcessor {
  public static void main(String[] args) {
    var processor = new StreamProcessor();
    var topology = processor.buildTopology();
    var properties = StreamProperties.newDefaultStreamProperties(UUID.randomUUID().toString());
    System.out.println(topology.describe());
    var streams = new KafkaStreams(topology, properties);
    streams.cleanUp();
    streams.setUncaughtExceptionHandler((thread, throwable) -> {
      throwable.printStackTrace();
      streams.close();
      System.exit(1);
    });
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public Topology buildTopology() {
    var builder = new StreamsBuilder();
    KStream<List<Integer>, StockQuantity> stockStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.stockquantity")
        .map(this::stripStockStateMetadata)
        .map(this::deserializeStockState);
    KTable<List<Integer>, Quantity> warehouseStock =
        stockStream
            .groupByKey(
                Serialized.with(
                    JsonSerde.of(new TypeReference<>() {}), JsonSerde.of(StockQuantity.class)))
            .aggregate(
                () -> Quantity.empty(LocalDateTime.MIN.atZone(ZoneOffset.UTC)),
                this::lastWriteWins,
                Materialized.with(
                    JsonSerde.of(new TypeReference<>() {}), JsonSerde.of(Quantity.class)));
    warehouseStock.toStream().to(
        "warehouse-stock",
        Produced.with(
            JsonSerde.of(new TypeReference<>() { }),
            JsonSerde.of(Quantity.class)));

    KStream<Integer, Warehouse> warehouseStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.warehouse")
        .map(this::stripWarehouseMetadata)
        .map(this::deserializeWarehouse);
    KTable<Integer, Quantity> warehouseQuantityTable = warehouseStock
        .groupBy((key, value) -> new KeyValue<>(key.get(0), value),
            Serialized.with(Serdes.Integer(), JsonSerde.of(Quantity.class)))
        .reduce(Quantity::sum, Quantity::subtract);
    KStream<WarehouseKey, Allocation> warehouseCapacity = warehouseStream
        .join(warehouseQuantityTable,
            WarehouseStockQuantity::new,
            Joined.with(Serdes.Integer(),
                JsonSerde.of(Warehouse.class),
                JsonSerde.of(Quantity.class)))
        .map((id, warehouseStockQuantity) -> {
          var warehouse = warehouseStockQuantity.getWarehouse();
          var qty = warehouseStockQuantity.getStockQuantity();
          var key = new WarehouseKey(id,
              warehouse.getName(),
              warehouse.getLatitude(),
              warehouse.getLongitude());
          var allocation = Allocation.calculate(qty.getQuantity(), warehouse.getStorageCapacity());
          return new KeyValue<>(key, allocation);
        });
    warehouseCapacity.to("warehouse-capacity",
        Produced.with(JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class)));

    KTable<Integer, Integer> globalStock =
        warehouseStock
            .groupBy(
                (warehouseItemPair, qty) -> new KeyValue<>(warehouseItemPair.get(1), qty),
                Serialized.with(Serdes.Integer(), JsonSerde.of(Quantity.class) ))
            .aggregate(
                () -> 0,
                (key, value, acc) -> value.getQuantity() + acc,
                (key, value, acc) -> acc - value.getQuantity(),
                Materialized.with(Serdes.Integer(), Serdes.Integer()));
    globalStock.toStream().to("global-stock", Produced.with(Serdes.Integer(), Serdes.Integer()));

    KGroupedTable<Integer, Integer> globalChanges = globalStock.groupBy(KeyValue::new);
    KTable<Integer, List<Integer>> globalStockMinMax = globalChanges
        .aggregate(() -> Lists.newArrayList(Integer.MAX_VALUE, Integer.MIN_VALUE),
            (key, value, acc) -> {
              int min = acc.get(0), max = acc.get(1);
              if (value < min) {
                min = value;
              }
              if (value > max) {
                max = value;
              }
              return Lists.newArrayList(min, max);
            }, (key, value, acc) -> acc, // No need to revert, we want historical min and max
            Materialized.with(Serdes.Integer(), JsonSerde.of(new TypeReference<List<Integer>>() {
            })));

    return builder.build();
  }

  private KeyValue<JsonNode, JsonNode> stripWarehouseMetadata(JsonNode key, JsonNode value) {
    var id = JsonNodeFactory.instance.numberNode(key.at("/payload/id").intValue());
    var currentState = value.at("/payload/after");
    return new KeyValue<>(id, currentState);
  }

  private KeyValue<List<Integer>, StockQuantity> deserializeStockState(JsonNode idsArray, JsonNode value) {
    var ids = SerializationUtils.deserialize(idsArray, new TypeReference<List<Integer>>() { });
    var stockState = SerializationUtils.deserialize(value, DbStockState.class);
    return new KeyValue<>(ids, stockState.toStockState());
  }

  private KeyValue<Integer, Warehouse> deserializeWarehouse(JsonNode jsonId, JsonNode jsonWarehouse) {
    int id = jsonId.asInt();
    var dbWarehouse = SerializationUtils.deserialize(jsonWarehouse, DbWarehouse.class);
    return new KeyValue<>(id, dbWarehouse.toWarehouse());
  }

  private Quantity lastWriteWins(List<Integer> ids, StockQuantity state, Quantity acc) {
    if (acc.getLastUpdate().isAfter(state.getLastUpdate())) {
      return acc;
    } else {
      return Quantity.of(state);
    }
  }

  private Quantity lastWriteWins(Quantity stockQuantity1, Quantity quantity) {
    if (stockQuantity1.getLastUpdate().isAfter(quantity.getLastUpdate())) {
      return stockQuantity1;
    } else {
      return quantity;
    }
  }

  private KeyValue<JsonNode, JsonNode> stripStockStateMetadata(JsonNode key, JsonNode value) {
    var currentState = value.at("/payload/after");
    ArrayNode ids = JsonNodeFactory.instance.arrayNode(2);
    ids.add(key.at("/payload/warehouseid"));
    ids.add(key.at("/payload/stockitemid"));
    return new KeyValue<>(ids, currentState);
  }

  private static class WarehouseStockQuantity {
    private final Warehouse warehouse;

    private final Quantity stockQuantity;

    public WarehouseStockQuantity(Warehouse warehouse,
        Quantity stockQuantity) {
      this.warehouse = warehouse;
      this.stockQuantity = stockQuantity;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WarehouseStockQuantity that = (WarehouseStockQuantity) o;
      return Objects.equals(warehouse, that.warehouse) &&
          Objects.equals(stockQuantity, that.stockQuantity);
    }

    @Override
    public int hashCode() {
      return Objects.hash(warehouse, stockQuantity);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("warehouse", warehouse)
          .add("stockQuantity", stockQuantity)
          .toString();
    }

    public Warehouse getWarehouse() {
      return warehouse;
    }

    public Quantity getStockQuantity() {
      return stockQuantity;
    }
  }
}
