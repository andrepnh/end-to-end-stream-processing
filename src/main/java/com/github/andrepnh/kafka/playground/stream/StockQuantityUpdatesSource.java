package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class StockQuantityUpdatesSource {
  private final KStream<List<Integer>, StockQuantity> stockUpdatesStream;

  private StockQuantityUpdatesSource(KStream<List<Integer>, StockQuantity> stockUpdatesStream) {
    this.stockUpdatesStream = checkNotNull(stockUpdatesStream);
  }

  public static StockQuantityUpdatesSource build(StreamsBuilder streamsBuilder) {
    KStream<List<Integer>, StockQuantity> stockStream = streamsBuilder
        .<JsonNode, JsonNode>stream("connect_test.public.stockquantity")
        .map(StockQuantityUpdatesSource::stripStockQuantityMetadata)
        .map(StockQuantityUpdatesSource::deserializeStockQuantity);
    return new StockQuantityUpdatesSource(stockStream);
  }

  public KStream<List<Integer>, StockQuantity> asStream() {
    return stockUpdatesStream;
  }

  private static KeyValue<JsonNode, JsonNode> stripStockQuantityMetadata(JsonNode key, JsonNode value) {
    var currentState = value.at("/after");
    ArrayNode ids = JsonNodeFactory.instance.arrayNode(2);
    ids.add(key.at("/warehouseid"));
    ids.add(key.at("/stockitemid"));
    return new KeyValue<>(ids, currentState);
  }

  private static KeyValue<List<Integer>, StockQuantity> deserializeStockQuantity(JsonNode idsArray, JsonNode value) {
    var ids = SerializationUtils.deserialize(idsArray, new TypeReference<List<Integer>>() { });
    var dbStockQuantity = SerializationUtils.deserialize(value, DbStockQuantity.class);
    return new KeyValue<>(ids, dbStockQuantity.toStockQuantity());
  }
}
