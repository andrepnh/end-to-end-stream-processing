package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class GlobalStockUpdatesSource {
  private final KStream<Integer, GlobalStockUpdate> globalStockUpdateStream;

  public GlobalStockUpdatesSource(KStream<Integer, GlobalStockUpdate> globalStockUpdateStream) {
    this.globalStockUpdateStream = checkNotNull(globalStockUpdateStream);
  }

  public static GlobalStockUpdatesSource from(WarehouseStockSource warehouseStockSource) {
    KTable<Integer, GlobalStockUpdate> globalStockUpdateTable =
        warehouseStockSource.asTable()
            .groupBy(
                (warehouseItemPair, qty) -> new KeyValue<>(warehouseItemPair.get(1), qty),
                Serialized.with(Serdes.Integer(), JsonSerde.of(Quantity.class)))
            .aggregate(
                GlobalStockUpdate::zero,
                (key, quantity, acc) -> acc.add(quantity),
                (key, quantity, acc) -> acc.subtract(quantity),
                Materialized.with(Serdes.Integer(), JsonSerde.of(GlobalStockUpdate.class)));
    var globalStockUpdateStream = globalStockUpdateTable.toStream();
    return new GlobalStockUpdatesSource(globalStockUpdateStream);
  }

  public KStream<Integer, GlobalStockUpdate> asStream() {
    return globalStockUpdateStream;
  }

  public void sinkTo(String targetTopic) {
    KStream<Integer, QuantityWrapper> globalStockStream = globalStockUpdateStream
        .map((id, quantity) -> new KeyValue<>(
            id,
            new QuantityWrapper(quantity)));
    globalStockStream.to(targetTopic, Produced.with(
        JsonSerde.of(Integer.class),
        JsonSerde.of(QuantityWrapper.class)));
  }

  static class QuantityWrapper {
    private final int quantity;

    private final ZonedDateTime lastUpdate;

    // For whatever reason the parameter names module did not work here
    public QuantityWrapper(@JsonProperty("quantity") int quantity,
        @JsonProperty("lastUpdate") ZonedDateTime lastUpdate) {
      this.quantity = quantity;
      this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC);
    }

    public QuantityWrapper(GlobalStockUpdate update) {
      this(update.getQuantity(), update.getLastUpdate());
    }

    public QuantityWrapper add(QuantityWrapper that) {
      var lastUpdate = that.getLastUpdate().isAfter(this.lastUpdate)
          ? that.getLastUpdate() : this.lastUpdate;
      return new QuantityWrapper(this.quantity + that.quantity, lastUpdate);
    }

    public int getQuantity() {
      return quantity;
    }

    public ZonedDateTime getLastUpdate() {
      return lastUpdate;
    }
  }

  public static class GlobalStockUpdate {
    private final int quantity;

    private final boolean previousUpdateRevert;

    private final ZonedDateTime lastUpdate;

    public GlobalStockUpdate(int quantity, boolean previousUpdateRevert, ZonedDateTime lastUpdate) {
      this.quantity = quantity;
      this.previousUpdateRevert = previousUpdateRevert;
      this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC);
    }

    public static GlobalStockUpdate zero() {
      return new GlobalStockUpdate(0, false, LocalDateTime.MIN.atZone(ZoneOffset.UTC));
    }

    public GlobalStockUpdate add(GlobalStockUpdate quantity) {
      var lastUpdate = quantity.getLastUpdate().isAfter(this.lastUpdate)
          ? quantity.getLastUpdate() : this.lastUpdate;
      return new GlobalStockUpdate(this.quantity + quantity.getQuantity(), false, lastUpdate);
    }

    public GlobalStockUpdate add(Quantity quantity) {
      var lastUpdate = quantity.getLastUpdate().isAfter(this.lastUpdate)
          ? quantity.getLastUpdate() : this.lastUpdate;
      return new GlobalStockUpdate(this.quantity + quantity.getQuantity(), false, lastUpdate);
    }

    public GlobalStockUpdate subtract(Quantity quantity) {
      var lastUpdate = quantity.getLastUpdate().isAfter(this.lastUpdate)
          ? quantity.getLastUpdate() : this.lastUpdate;
      return new GlobalStockUpdate(this.quantity - quantity.getQuantity(), true, lastUpdate);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("quantity", quantity)
          .add("previousUpdateRevert", previousUpdateRevert)
          .add("lastUpdate", lastUpdate)
          .toString();
    }

    public int getQuantity() {
      return quantity;
    }

    public ZonedDateTime getLastUpdate() {
      return lastUpdate;
    }

    /**
     * @return true if this update is internally used to revert a previous one
     */
    public boolean isPreviousUpdateRevert() {
      return previousUpdateRevert;
    }
  }
}
