package com.github.andrepnh.kafka.playground.db.gen;

import com.google.common.base.MoreObjects;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class StockQuantity {
  private final int warehouseId;

  private final int stockItemId;

  private final int quantity;

  private final ZonedDateTime lastUpdate;

  public StockQuantity(int warehouseId, int stockItemId, int quantity, ZonedDateTime lastUpdate) {
    this.warehouseId = warehouseId;
    this.stockItemId = stockItemId;
    this.quantity = quantity;
    // Nanosecond precision is not available
    this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC).withNano(0);
  }

  public static StockQuantity random(int warehouseId, int stockItemId) {
    return new StockQuantity(
        warehouseId,
        stockItemId,
        Generator.positive(2000),
        ZonedDateTime.now(ZoneOffset.UTC));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StockQuantity that = (StockQuantity) o;
    return warehouseId == that.warehouseId &&
        stockItemId == that.stockItemId &&
        quantity == that.quantity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(warehouseId, stockItemId, quantity);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("warehouseId", warehouseId)
        .add("stockItemId", stockItemId)
        .add("quantity", quantity)
        .add("lastUpdate", lastUpdate)
        .toString();
  }

  public int getWarehouseId() {
    return warehouseId;
  }

  public int getStockItemId() {
    return stockItemId;
  }

  public int getQuantity() {
    return quantity;
  }

  public ZonedDateTime getLastUpdate() {
    return lastUpdate;
  }
}
