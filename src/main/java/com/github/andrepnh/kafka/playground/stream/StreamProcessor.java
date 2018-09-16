package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.Function;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public class StreamProcessor {
  public static void main(String[] args) {
    var processor = new StreamProcessor();
    var builder = new StreamsBuilder();
    Function<Integer, JsonNode> numericNodeFactory = JsonNodeFactory.instance::numberNode;
    KStream<JsonNode, JsonNode> stockStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.stockstate")
        .map(processor::stripMetadata);
    KStream<JsonNode, JsonNode> warehouseStock = stockStream
        .groupByKey()
        .reduce(processor::maxByLastUpdate)
        .toStream();
    warehouseStock.to("warehouse-stock");
    KTable<JsonNode, JsonNode> globalStock = warehouseStock
        .groupBy((warehouseItemPair, qty) -> warehouseItemPair.get(1))
        .aggregate(() -> numericNodeFactory.apply(0),
            (key, value, acc) -> numericNodeFactory.apply(calculateHardQuantity(value) + acc.asInt()));
    globalStock.toStream().to("global-stock");

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

  private static int calculateHardQuantity(JsonNode qty) {
    return qty.path("supply").asInt() - qty.path("demand").asInt() - qty.path("reserved").asInt();
  }

  private JsonNode maxByLastUpdate(JsonNode node1, JsonNode node2) {
    var mostRecentUpdate = getLastUpdate(node1).isAfter(getLastUpdate(node2))
        ? node1.deepCopy() : node2.deepCopy();
    ((ObjectNode) mostRecentUpdate).remove("warehouseid");
    ((ObjectNode) mostRecentUpdate).remove("stockitemid");
    return mostRecentUpdate;
  }

  private ZonedDateTime getLastUpdate(JsonNode node) {
    long epochMilli = node.path("lastUpdate").asLong();
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(epochMilli).atZone(ZoneOffset.UTC);
    return zonedDateTime;
  }

  private KeyValue<JsonNode, JsonNode> stripMetadata(JsonNode key, JsonNode value) {
    var currentState = value.at("/payload/after");
    ArrayNode ids = JsonNodeFactory.instance.arrayNode(2);
    ids.add(key.at("/payload/warehouseid"));
    ids.add(key.at("/payload/stockitemid"));
    return new KeyValue<>(ids, currentState);
  }
}
