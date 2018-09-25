package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.andrepnh.kafka.playground.db.gen.StockState;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Joined;
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
    KStream<List<Integer>, StockState> stockStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.stockstate")
        .map(this::stripStockStateMetadata)
        .map(this::deserializeStockState);
    KTable<List<Integer>, StockQuantity> warehouseStock =
        stockStream
            .groupByKey(
                Serialized.with(
                    JsonSerde.of(new TypeReference<>() {}), JsonSerde.of(StockState.class)))
            .aggregate(
                () -> StockQuantity.empty(LocalDateTime.MIN.atZone(ZoneOffset.UTC)),
                this::lastWriteWins,
                Materialized.with(
                    JsonSerde.of(new TypeReference<>() {}), JsonSerde.of(StockQuantity.class)));
    warehouseStock.toStream().to(
        "warehouse-stock",
        Produced.with(
            JsonSerde.of(new TypeReference<>() { }),
            JsonSerde.of(StockQuantity.class)));

    KStream<Integer, Warehouse> warehouseStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.warehouse")
        .map(this::stripWarehouseMetadata)
        .map(this::deserializeWarehouse);
    KTable<Integer, StockQuantity> warehouseQuantityTable = warehouseStock
        .groupBy((key, value) -> new KeyValue<>(key.get(0), value),
            Serialized.with(Serdes.Integer(), JsonSerde.of(StockQuantity.class)))
        .reduce(StockQuantity::sum, StockQuantity::subtract);
    KStream<WarehouseKey, Allocation> warehouseCapacity = warehouseStream
        .join(warehouseQuantityTable,
            WarehouseStockQuantity::new,
            Joined.with(Serdes.Integer(),
                JsonSerde.of(Warehouse.class),
                JsonSerde.of(StockQuantity.class)))
        .map((id, warehouseStockQuantity) -> {
          var warehouse = warehouseStockQuantity.getWarehouse();
          var qty = warehouseStockQuantity.getStockQuantity();
          var key = new WarehouseKey(id,
              warehouse.getName(),
              warehouse.getLatitude(),
              warehouse.getLongitude());
          var allocation = new Allocation(
              (double) qty.softDemand() / warehouse.getStorageCapacity(),
              (double) qty.hardDemand() / warehouse.getStorageCapacity());
          return new KeyValue<>(key, allocation);
        });
    warehouseCapacity.to("warehouse-capacity",
        Produced.with(JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class)));

    KTable<Integer, Integer> globalStock =
        warehouseStock
            .groupBy(
                (warehouseItemPair, qty) -> new KeyValue<>(warehouseItemPair.get(1), qty),
                Serialized.with(Serdes.Integer(), JsonSerde.of(StockQuantity.class) ))
            .aggregate(
                () -> 0,
                (key, value, acc) -> value.hardQuantity() + acc,
                (key, value, acc) -> acc - value.hardQuantity(),
                Materialized.with(Serdes.Integer(), Serdes.Integer()));
    globalStock.toStream().to("global-stock", Produced.with(Serdes.Integer(), Serdes.Integer()));
    return builder.build();
  }

  private KeyValue<JsonNode, JsonNode> stripWarehouseMetadata(JsonNode key, JsonNode value) {
    var id = JsonNodeFactory.instance.numberNode(key.at("/payload/id").intValue());
    var currentState = value.at("/payload/after");
    return new KeyValue<>(id, currentState);
  }

  private KeyValue<List<Integer>, StockState> deserializeStockState(JsonNode idsArray, JsonNode value) {
    var ids = SerializationUtils.deserialize(idsArray, new TypeReference<List<Integer>>() { });
    var stockState = SerializationUtils.deserialize(value, DbStockState.class);
    return new KeyValue<>(ids, stockState.toStockState());
  }

  private KeyValue<Integer, Warehouse> deserializeWarehouse(JsonNode jsonId, JsonNode jsonWarehouse) {
    int id = jsonId.asInt();
    var dbWarehouse = SerializationUtils.deserialize(jsonWarehouse, DbWarehouse.class);
    return new KeyValue<>(id, dbWarehouse.toWarehouse());
  }

  private StockQuantity lastWriteWins(List<Integer> ids, StockState state, StockQuantity acc) {
    if (acc.getLastUpdate().isAfter(state.getLastUpdate())) {
      return acc;
    } else {
      return StockQuantity.of(state);
    }
  }

  private StockQuantity lastWriteWins(StockQuantity stockQuantity1, StockQuantity stockQuantity2) {
    if (stockQuantity1.getLastUpdate().isAfter(stockQuantity2.getLastUpdate())) {
      return stockQuantity1;
    } else {
      return stockQuantity2;
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

    private final StockQuantity stockQuantity;

    public WarehouseStockQuantity(Warehouse warehouse,
        StockQuantity stockQuantity) {
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

    public StockQuantity getStockQuantity() {
      return stockQuantity;
    }
  }
}
