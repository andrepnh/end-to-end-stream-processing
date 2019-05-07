package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.andrepnh.kafka.playground.stream.GlobalStockUpdatesSource.GlobalStockUpdate;
import com.google.common.collect.Lists;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class StockGlobalPercentageSource {
  private final KStream<Integer, PercentageWrapper> globalPercentagePerItem;

  public StockGlobalPercentageSource(KStream<Integer, PercentageWrapper> globalPercentagePerItem) {
    this.globalPercentagePerItem = checkNotNull(globalPercentagePerItem);
  }

  public static StockGlobalPercentageSource from(GlobalStockUpdatesSource globalStockUpdatesSource) {
    KStream<Integer, GlobalStockUpdate> globalStockUpdatesStream = globalStockUpdatesSource.asStream();
    KTable<Integer, List<Integer>> globalStockMinMax = globalStockUpdatesStream
        .groupByKey()
        .aggregate(() -> Lists.newArrayList(Integer.MAX_VALUE, Integer.MIN_VALUE),
            (key, value, acc) -> {
              int min = acc.get(0), max = acc.get(1);
              if (value.isPreviousUpdateRevert()) {
                // We ignore reversions because they can drop the minimum to an incorrect value.
                // Suppose the min is initially set to 200 and global is updated to 100. This will
                // first reverse the 200 update, dropping value to 0 and stopping the 100 update
                // from becoming the minimum.
                return Lists.newArrayList(min, max);
              }
              min = Math.min(value.getQuantity(), min);
              max = Math.max(value.getQuantity(), max);
              return Lists.newArrayList(min, max);
            }, Materialized.with(Serdes.Integer(), JsonSerde.of(new TypeReference<>() { })));
    KStream<Integer, PercentageWrapper> globalPercentagePerItem = globalStockUpdatesStream
        .join(globalStockMinMax, (global, minMax) -> {
          int min = minMax.get(0), max = minMax.get(1);
          int offset = global.getQuantity() - min, maxOffset = max - min;
          return new PercentageWrapper((double) offset / maxOffset, global.getLastUpdate());
        }, Joined.with(Serdes.Integer(),
            JsonSerde.of(GlobalStockUpdate.class),
            JsonSerde.of(new TypeReference<>() { })))
        .filter((id, percentageWrapper) -> Double.isFinite(percentageWrapper.getPercentage()));
    return new StockGlobalPercentageSource(globalPercentagePerItem);
  }

  public void sinkTo(String targetTopic) {
    globalPercentagePerItem.to(
        targetTopic,
        Produced.with(JsonSerde.of(Integer.class), JsonSerde.of(PercentageWrapper.class)));
  }

  public static class PercentageWrapper {
    private final double percentage;

    private final ZonedDateTime lastUpdate;

    // For whatever reason the parameter names module did not work here
    public PercentageWrapper(@JsonProperty("percentage") double percentage,
        @JsonProperty("lastUpdate") ZonedDateTime lastUpdate) {
      this.percentage = percentage;
      this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC);
    }

    public double getPercentage() {
      return percentage;
    }

    public ZonedDateTime getLastUpdate() {
      return lastUpdate;
    }
  }
}
