package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class WarehouseStockSource {
  private final KTable<List<Integer>, Quantity> warehouseStockTable;

  private WarehouseStockSource(KTable<List<Integer>, Quantity> warehouseStockTable) {
    this.warehouseStockTable = checkNotNull(warehouseStockTable);
    var warehouseStockStream = warehouseStockTable.toStream();
    warehouseStockStream.to(
        "warehouse-stock",
        Produced.with(
            JsonSerde.of(new TypeReference<>() { }),
            JsonSerde.of(Quantity.class)));
  }

  public static WarehouseStockSource from(StockQuantityUpdatesSource stockQuantitySource) {
    KTable<List<Integer>, Quantity> warehouseStock = stockQuantitySource.asStream()
        .groupByKey(
            Serialized.with(
                JsonSerde.of(new TypeReference<>() {}), JsonSerde.of(StockQuantity.class)))
        .aggregate(
            () -> Quantity.empty(LocalDateTime.MIN.atZone(ZoneOffset.UTC)),
            WarehouseStockSource::lastWriteWins,
            Materialized.with(
                JsonSerde.of(new TypeReference<>() {}), JsonSerde.of(Quantity.class)));
    return new WarehouseStockSource(warehouseStock);
  }

  public KTable<List<Integer>, Quantity> asTable() {
    return warehouseStockTable;
  }

  private static Quantity lastWriteWins(List<Integer> ids, StockQuantity state, Quantity acc) {
    if (acc.getLastUpdate().isAfter(state.getLastUpdate())) {
      return acc;
    } else {
      return Quantity.of(state);
    }
  }
}
