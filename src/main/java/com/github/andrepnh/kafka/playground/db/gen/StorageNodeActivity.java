package com.github.andrepnh.kafka.playground.db.gen;

import com.google.common.base.MoreObjects;
import java.time.ZonedDateTime;
import java.util.Objects;

public class StorageNodeActivity {
  private final int stockItemId;

  private final int storageNodeId;

  private final ZonedDateTime moment;

  private final int quantity;

  public StorageNodeActivity(
      int storageNodeId, int stockItemId, ZonedDateTime moment, int quantity) {
    this.stockItemId = stockItemId;
    this.storageNodeId = storageNodeId;
    this.moment = moment;
    this.quantity = quantity;
  }

  public static StorageNodeActivity random(int storageNodeId, int stockItemId) {
    return new StorageNodeActivity(
        storageNodeId, stockItemId, Generator.moment(), Generator.rangeClosed(-500, 500));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StorageNodeActivity that = (StorageNodeActivity) o;
    return stockItemId == that.stockItemId
        && storageNodeId == that.storageNodeId
        && quantity == that.quantity
        && Objects.equals(moment, that.moment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stockItemId, storageNodeId, moment, quantity);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("stockItemId", stockItemId)
        .add("storageNodeId", storageNodeId)
        .add("moment", moment)
        .add("quantity", quantity)
        .toString();
  }

  public int getStockItemId() {
    return stockItemId;
  }

  public int getStorageNodeId() {
    return storageNodeId;
  }

  public ZonedDateTime getMoment() {
    return moment;
  }

  public int getQuantity() {
    return quantity;
  }
}
