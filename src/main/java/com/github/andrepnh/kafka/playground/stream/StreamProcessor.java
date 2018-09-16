package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.andrepnh.kafka.playground.db.gen.StockState;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class StreamProcessor {
  public static void main(String[] args) {
    var processor = new StreamProcessor();
    var builder = new StreamsBuilder();
    KStream<List<Integer>, StockState> stockStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.stockstate")
        .map(processor::stripMetadata)
        .map(processor::deserialize);
    KStream<List<Integer>, StockQuantity> warehouseStock =
        stockStream
            .groupByKey(
                Serialized.with(
                    JsonSerde.of(new TypeReference<List<Integer>>() {}), JsonSerde.of(StockState.class)))
            .aggregate(
                () -> StockQuantity.empty(LocalDateTime.MIN.atZone(ZoneOffset.UTC)),
                processor::lastWriteWins,
                Materialized.with(
                    JsonSerde.of(new TypeReference<List<Integer>>() {}), JsonSerde.of(StockQuantity.class)))
            .toStream();
    warehouseStock.to(
        "warehouse-stock",
        Produced.with(
            JsonSerde.of(new TypeReference<List<Integer>>() { }),
            JsonSerde.of(StockQuantity.class)));
    KTable<Integer, Integer> globalStock =
        warehouseStock
            .groupBy(
                (warehouseItemPair, qty) -> warehouseItemPair.get(1),
                Serialized.with(Serdes.Integer(), JsonSerde.of(StockQuantity.class) ))
            .aggregate(
                () -> 0,
                (key, value, acc) -> value.hardQuantity() + acc,
                Materialized.with(Serdes.Integer(), Serdes.Integer()));
    globalStock.toStream().to("global-stock", Produced.with(Serdes.Integer(), Serdes.Integer()));

    var properties = StreamProperties.newDefaultStreamProperties(UUID.randomUUID().toString());
    Topology topology = builder.build();
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

  private KeyValue<List<Integer>, StockState> deserialize(JsonNode idsArray, JsonNode value) {
    var ids = SerializationUtils.deserialize(idsArray, new TypeReference<List<Integer>>() { });
    var stockState = SerializationUtils.deserialize(value, DbStockState.class);
    return new KeyValue<>(ids, stockState.toStockState());
  }

  private StockQuantity lastWriteWins(List<Integer> ids, StockState state, StockQuantity acc) {
    if (acc.getLastUpdate().isAfter(state.getLastUpdate())) {
      return acc;
    } else {
      return StockQuantity.of(state);
    }
  }

  private KeyValue<JsonNode, JsonNode> stripMetadata(JsonNode key, JsonNode value) {
    var currentState = value.at("/payload/after");
    ArrayNode ids = JsonNodeFactory.instance.arrayNode(2);
    ids.add(key.at("/payload/warehouseid"));
    ids.add(key.at("/payload/stockitemid"));
    return new KeyValue<>(ids, currentState);
  }
}
