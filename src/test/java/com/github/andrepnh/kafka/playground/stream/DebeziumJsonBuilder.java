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
    ObjectNode key = JsonNodeFactory.instance.objectNode(),
        keyPayload = key.deepCopy();
    keyPayload.set("id", JsonNodeFactory.instance.numberNode(warehouse.getId()));
    key.set("payload", keyPayload);

    ObjectNode value = JsonNodeFactory.instance.objectNode(),
        valuePayload = value.deepCopy(),
        valuePayloadAfter = value.deepCopy();
    valuePayloadAfter.set("id", JsonNodeFactory.instance.numberNode(warehouse.getId()));
    valuePayloadAfter.set("name", JsonNodeFactory.instance.textNode(warehouse.getName()));
    valuePayloadAfter.set("latitude", JsonNodeFactory.instance.numberNode(warehouse.getLatitude()));
    valuePayloadAfter.set("longitude", JsonNodeFactory.instance.numberNode(warehouse.getLongitude()));
    valuePayloadAfter.set("storagecapacity", JsonNodeFactory.instance.numberNode(warehouse.getStorageCapacity()));
    valuePayload.set("after", valuePayloadAfter);
    value.set("payload", valuePayload);

    return new KeyValue<>(key, value);
  }

  private KeyValue<JsonNode, JsonNode> toKeyValue(StockQuantity stockQuantity) {
    ObjectNode key = JsonNodeFactory.instance.objectNode(),
        keyPayload = key.deepCopy();
    keyPayload.set("warehouseid", JsonNodeFactory.instance.numberNode(stockQuantity.getWarehouseId()));
    keyPayload.set("stockitemid", JsonNodeFactory.instance.numberNode(stockQuantity.getStockItemId()));
    key.set("payload", keyPayload);

    ObjectNode value = JsonNodeFactory.instance.objectNode(),
        valuePayload = value.deepCopy(),
        valuePayloadAfter = value.deepCopy();
    valuePayloadAfter.set("warehouseid", JsonNodeFactory.instance.numberNode(stockQuantity.getWarehouseId()));
    valuePayloadAfter.set("stockitemid", JsonNodeFactory.instance.numberNode(stockQuantity.getStockItemId()));
    valuePayloadAfter.set("quantity", JsonNodeFactory.instance.numberNode(stockQuantity.getQuantity()));
    valuePayloadAfter.set("lastupdate", JsonNodeFactory.instance.numberNode(
        stockQuantity.getLastUpdate().toInstant().toEpochMilli()));
    valuePayload.set("after", valuePayloadAfter);
    value.set("payload", valuePayload);

    return new KeyValue<>(key, value);
  }


}
