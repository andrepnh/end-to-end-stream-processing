package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.streams.KeyValue;

public class DebeziumJsonBuilder {
  private final ImmutableList.Builder<KeyValue<JsonNode, JsonNode>> records = ImmutableList.builder();

  public DebeziumJsonBuilder add(StockQuantity stockQuantity) {
    records.add(toKeyValue(stockQuantity));
    return this;
  }

  public DebeziumJsonBuilder add(Warehouse warehouse) {
    records.add(toKeyValue(warehouse));
    return this;
  }

  public ImmutableList<KeyValue<JsonNode, JsonNode>> build() {
    return records.build();
  }

  private KeyValue<JsonNode, JsonNode> toKeyValue(Warehouse warehouse) {
    ObjectNode key = JsonNodeFactory.instance.objectNode();
    key.set("id", JsonNodeFactory.instance.numberNode(warehouse.getId()));

    ObjectNode value = JsonNodeFactory.instance.objectNode(),
        valueAfter = value.deepCopy();
    valueAfter.set("id", JsonNodeFactory.instance.numberNode(warehouse.getId()));
    valueAfter.set("name", JsonNodeFactory.instance.textNode(warehouse.getName()));
    valueAfter.set("latitude", JsonNodeFactory.instance.numberNode(warehouse.getLatitude()));
    valueAfter.set("longitude", JsonNodeFactory.instance.numberNode(warehouse.getLongitude()));
    valueAfter.set("storagecapacity", JsonNodeFactory.instance.numberNode(warehouse.getStorageCapacity()));
    value.set("after", valueAfter);

    return new KeyValue<>(key, value);
  }

  private KeyValue<JsonNode, JsonNode> toKeyValue(StockQuantity stockQuantity) {
    ObjectNode key = JsonNodeFactory.instance.objectNode();
    key.set("warehouseid", JsonNodeFactory.instance.numberNode(stockQuantity.getWarehouseId()));
    key.set("stockitemid", JsonNodeFactory.instance.numberNode(stockQuantity.getStockItemId()));

    ObjectNode value = JsonNodeFactory.instance.objectNode(),
        valueAfter = value.deepCopy();
    valueAfter.set("warehouseid", JsonNodeFactory.instance.numberNode(stockQuantity.getWarehouseId()));
    valueAfter.set("stockitemid", JsonNodeFactory.instance.numberNode(stockQuantity.getStockItemId()));
    valueAfter.set("quantity", JsonNodeFactory.instance.numberNode(stockQuantity.getQuantity()));
    valueAfter.set("lastupdate", JsonNodeFactory.instance.numberNode(
        stockQuantity.getLastUpdate().toInstant().toEpochMilli()));
    value.set("after", valueAfter);

    return new KeyValue<>(key, value);
  }


}
