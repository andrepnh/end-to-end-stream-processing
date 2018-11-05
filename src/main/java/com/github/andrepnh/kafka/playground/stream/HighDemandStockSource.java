package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class HighDemandStockSource {
  private final KTable<Integer, Quantity> globalDemandTable;

  public HighDemandStockSource(KTable<Integer, Quantity> globalDemandTable) {
    this.globalDemandTable = checkNotNull(globalDemandTable);
  }

  public static HighDemandStockSource from(StockQuantityUpdatesSource stockUpdatesSource) {
    KTable<List<Integer>, Quantity> demandTable = stockUpdatesSource.asStream()
        .filter((warehouseItemPair, quantity) -> quantity.getQuantity() < 0)
        .groupByKey(Serialized.with(
            JsonSerde.of(new TypeReference<>() { }),
            JsonSerde.of(StockQuantity.class)))
        .aggregate(
            () -> Quantity.empty(LocalDateTime.MIN.atZone(ZoneOffset.UTC)),
            HighDemandStockSource::lastWriteWins,
            Materialized.with(JsonSerde.of(new TypeReference<>() { }), JsonSerde.of(Quantity.class)));
    KTable<Integer, Quantity> globalDemandTable = demandTable.groupBy(
        (warehouseItemPair, quantity) -> new KeyValue<>(warehouseItemPair.get(1), quantity),
        Serialized.with(Serdes.Integer(), JsonSerde.of(Quantity.class)))
        .reduce(Quantity::add,
            Quantity::subtract,
            Materialized.with(Serdes.Integer(), JsonSerde.of(Quantity.class)));
    return new HighDemandStockSource(globalDemandTable);
  }

  public void sinkTo(String targetTopic) {
    globalDemandTable
        .toStream()
        .to(targetTopic, Produced.with(JsonSerde.of(Integer.class), JsonSerde.of(Quantity.class)));
  }

  private static Quantity lastWriteWins(List<Integer> ids, StockQuantity state, Quantity acc) {
    if (acc.getLastUpdate().isAfter(state.getLastUpdate())) {
      return acc;
    } else {
      return Quantity.of(state);
    }
  }
}
