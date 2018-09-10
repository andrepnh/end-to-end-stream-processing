package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.ZonedDateTime;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.eclipse.collections.api.tuple.primitive.IntIntPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

public class StreamProcessor {
  public static void main(String[] args) {
    var processor = new StreamProcessor();
    var builder = new StreamsBuilder();
    KStream<JsonNode, JsonNode> stockStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.stockstate")
        .map(processor::stripMetadata);
    KTable<JsonNode, JsonNode> globalStock = stockStream
        .groupBy((warehouseItemPair, stockState) -> warehouseItemPair.get(1))
        .reduce(processor::maxByLastUpdate);

    var properties = StreamProperties.newDefaultStreamProperties("teste");
    Topology topology = builder.build();
    System.out.println(topology);
    var streams = new KafkaStreams(topology, properties);
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private JsonNode maxByLastUpdate(JsonNode node1, JsonNode node2) {
    return getLastUpdate(node1).isAfter(getLastUpdate(node2)) ? node1 : node2;
  }

  private ZonedDateTime getLastUpdate(JsonNode node) {
    String lastUpdate = node.path("lastUpdate").asText();
    return ZonedDateTime.parse(lastUpdate);
  }

  private KeyValue<JsonNode, JsonNode> stripMetadata(JsonNode key, JsonNode value) {
    var currentState = value.at("/payload/after");
    ArrayNode ids = JsonNodeFactory.instance.arrayNode(2);
    ids.add(key.at("/payload/warehouseId"));
    ids.add(key.at("/payload/stockItemId"));
    return new KeyValue<>(ids, currentState);
  }
}
