package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.google.common.base.MoreObjects;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class WarehouseAllocationSource {
  private final KStream<Integer, WarehouseAllocation> warehouseAllocationStream;

  private WarehouseAllocationSource(KStream<Integer, WarehouseAllocation> warehouseAllocationStream) {
    this.warehouseAllocationStream = checkNotNull(warehouseAllocationStream);
  }

  public static WarehouseAllocationSource build(StreamsBuilder builder, WarehouseStockSource warehouseStockSource) {
    checkNotNull(builder);
    checkNotNull(warehouseStockSource);
    KStream<Integer, Warehouse> warehouseStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.warehouse")
        .map(WarehouseAllocationSource::stripWarehouseMetadata)
        .map(WarehouseAllocationSource::deserializeWarehouse);
    KTable<Integer, Quantity> warehouseQuantityTable = warehouseStockSource.asTable()
        .groupBy((key, value) -> new KeyValue<>(key.get(0), value),
            Serialized.with(Serdes.Integer(), JsonSerde.of(Quantity.class)))
        .reduce(Quantity::add, Quantity::subtract);
    KTable<Integer, Warehouse> warehouseTable = warehouseStream
        .groupByKey(Serialized.with(Serdes.Integer(), JsonSerde.of(Warehouse.class)))
        .reduce(WarehouseAllocationSource::lastWriteWins);
    KTable<Integer, WarehouseStockQuantity> warehouseStockQtyTable = warehouseTable
        .join(warehouseQuantityTable,
            WarehouseStockQuantity::new,
            Materialized.with(Serdes.Integer(), JsonSerde.of(WarehouseStockQuantity.class)));
    var warehouseStockQtyStream = warehouseStockQtyTable.toStream();
    KStream<Integer, WarehouseAllocation> warehouseAllocationStream = warehouseStockQtyStream
        .map((id, warehouseStockQuantity) -> {
          var warehouse = warehouseStockQuantity.getWarehouse();
          var qty = warehouseStockQuantity.getStockQuantity();
          var allocation = WarehouseAllocation.calculate(
              warehouse.getName(), warehouse.getLatitude(), warehouse.getLongitude(),
              qty.getQuantity(), warehouse.getStorageCapacity(), qty.getLastUpdate());
          return new KeyValue<>(id, allocation);
        });

    return new WarehouseAllocationSource(warehouseAllocationStream);
  }

  public void sinkTo(String targetTopic) {
    warehouseAllocationStream.to(targetTopic,
        Produced.with(JsonSerde.of(Integer.class), JsonSerde.of(WarehouseAllocation.class)));
  }

  private static KeyValue<JsonNode, JsonNode> stripWarehouseMetadata(JsonNode key, JsonNode value) {
    var id = JsonNodeFactory.instance.numberNode(key.at("/id").intValue());
    var currentState = value.at("/after");
    return new KeyValue<>(id, currentState);
  }

  private static KeyValue<Integer, Warehouse> deserializeWarehouse(JsonNode jsonId, JsonNode jsonWarehouse) {
    int id = jsonId.asInt();
    var dbWarehouse = SerializationUtils.deserialize(jsonWarehouse, DbWarehouse.class);
    return new KeyValue<>(id, dbWarehouse.toWarehouse());
  }

  private static Warehouse lastWriteWins(Warehouse warehouse1, Warehouse warehouse2) {
    return warehouse1.getLastUpdate().isAfter(warehouse2.getLastUpdate())
        ? warehouse1 : warehouse2;
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
