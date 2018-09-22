package com.github.andrepnh.kafka.playground.db.gen;

import com.google.common.base.MoreObjects;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class StockState {
  private final int warehouseId;

  private final int stockItemId;

  private final int supply;

  private final int demand;

  private final int reserved;

  private final ZonedDateTime lastUpdate;

  public StockState(int warehouseId, int stockItemId, int supply, int demand, int reserved, ZonedDateTime lastUpdate) {
    this.warehouseId = warehouseId;
    this.stockItemId = stockItemId;
    this.supply = supply;
    this.demand = demand;
    this.reserved = reserved;
    // Nanosecond precision is not available
    this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC).withNano(0);
  }

  public static StockState random(int warehouseId, int stockItemId) {
    return new StockState(
        warehouseId,
        stockItemId,
        Generator.positive(2000),
        Generator.positive(1000),
        Generator.positive(750),
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
    StockState that = (StockState) o;
    return warehouseId == that.warehouseId &&
        stockItemId == that.stockItemId &&
        supply == that.supply &&
        demand == that.demand &&
        reserved == that.reserved;
  }

  @Override
  public int hashCode() {
    return Objects.hash(warehouseId, stockItemId, supply, demand, reserved);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("warehouseId", warehouseId)
        .add("stockItemId", stockItemId)
        .add("supply", supply)
        .add("demand", demand)
        .add("reserved", reserved)
        .add("lastUpdate", lastUpdate)
        .toString();
  }

  public int getWarehouseId() {
    return warehouseId;
  }

  public int getStockItemId() {
    return stockItemId;
  }

  public int getSupply() {
    return supply;
  }

  public int getDemand() {
    return demand;
  }

  public int getReserved() {
    return reserved;
  }

  public ZonedDateTime getLastUpdate() {
    return lastUpdate;
  }
}
