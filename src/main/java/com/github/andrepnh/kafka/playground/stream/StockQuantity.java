package com.github.andrepnh.kafka.playground.stream;

import com.github.andrepnh.kafka.playground.db.gen.StockState;
import com.google.common.base.MoreObjects;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class StockQuantity {
  private final int supply;

  private final int demand;

  private final int reserved;

  private final ZonedDateTime lastUpdate;

  private StockQuantity(int supply, int demand, int reserved, ZonedDateTime lastUpdate) {
    this.supply = supply;
    this.demand = demand;
    this.reserved = reserved;
    this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC);
  }

  public static StockQuantity empty(ZonedDateTime when) {
    return new StockQuantity(0, 0, 0, when);
  }

  public static StockQuantity of(StockState state) {
    return new StockQuantity(state.getSupply(), state.getDemand(), state.getReserved(), state.getLastUpdate());
  }

  public StockQuantity sum(StockQuantity other) {
    return new StockQuantity(
        this.supply + other.supply,
        this.demand + other.demand,
        this.reserved + other.reserved,
        this.lastUpdate.isAfter(other.lastUpdate) ? this.lastUpdate : other.lastUpdate);
  }

  public int hardDemand() {
    return demand + reserved;
  }

  public int softDemand() {
    return demand;
  }

  public int hardQuantity() {
    return supply - demand - reserved;
  }

  public int softQuantity() {
    return supply - demand;
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
    return supply == that.supply &&
        demand == that.demand &&
        reserved == that.reserved &&
        Objects.equals(lastUpdate, that.lastUpdate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(supply, demand, reserved, lastUpdate);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("supply", supply)
        .add("demand", demand)
        .add("reserved", reserved)
        .add("lastUpdate", lastUpdate)
        .toString();
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
