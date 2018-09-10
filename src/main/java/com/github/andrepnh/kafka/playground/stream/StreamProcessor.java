package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.eclipse.collections.api.tuple.primitive.IntIntPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

public class StreamProcessor {
  public static void main(String[] args) {
    var processor = new StreamProcessor();
    var builder = new StreamsBuilder();
    KStream<IntIntPair, Integer> supplyStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.stocksupply")
        .map(processor::stripMetadata);
    KStream<IntIntPair, Integer> demandStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.stockdemand")
        .map(processor::stripMetadata);
    KStream<IntIntPair, Integer> reservationStream = builder
        .<JsonNode, JsonNode>stream("connect_test.public.stockreservation")
        .map(processor::stripMetadata);

    supplyStream.join(demandStream, (supply, demand) -> supply - demand, JoinWindows.of(5000))
        .join(reservationStream,
            (softQuantity, reservation) -> new StockState(softQuantity - reservation, softQuantity),

            JoinWindows.of(5000));
  }

  private KeyValue<IntIntPair, Integer> stripMetadata(JsonNode key, JsonNode value) {
    int warehouseId = key.at("/payload/after/warehouseId").asInt(),
        stockItemId = key.at("/payload/after/stockItemId").asInt();
    var quantity = value.at("/payload/after/quantity").asInt();
    return new KeyValue<>(PrimitiveTuples.pair(warehouseId, stockItemId), quantity);
  }

  private static class StockState {
    private final int quantity;

    private final int softQuantity;

    public StockState(int quantity, int softQuantity) {
      this.quantity = quantity;
      this.softQuantity = softQuantity;
    }

    public int getQuantity() {
      return quantity;
    }

    public int getSoftQuantity() {
      return softQuantity;
    }
  }
}
