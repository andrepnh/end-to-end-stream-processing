package com.github.andrepnh.kafka.playground.stream;

import com.github.andrepnh.kafka.playground.db.StockQuantity;
import com.google.common.base.MoreObjects;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Quantity {
  private final int quantity;

  private final ZonedDateTime lastUpdate;

  private Quantity(int quantity, ZonedDateTime lastUpdate) {
    this.quantity = quantity;
    this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC);
  }

  public static Quantity empty(ZonedDateTime when) {
    return new Quantity(0, when);
  }

  public static Quantity of(StockQuantity state) {
    return new Quantity(state.getQuantity(), state.getLastUpdate());
  }

  public Quantity add(Quantity other) {
    return new Quantity(
        this.quantity + other.quantity,
        this.lastUpdate.isAfter(other.lastUpdate) ? this.lastUpdate : other.lastUpdate);
  }

  public Quantity subtract(Quantity other) {
    return new Quantity(this.quantity - other.quantity, this.lastUpdate);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Quantity that = (Quantity) o;
    return quantity == that.quantity && Objects.equals(lastUpdate, that.lastUpdate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(quantity, lastUpdate);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("quantity", quantity)
        .add("lastUpdate", lastUpdate)
        .toString();
  }

  public int getQuantity() {
    return quantity;
  }

  public ZonedDateTime getLastUpdate() {
    return lastUpdate;
  }
}
