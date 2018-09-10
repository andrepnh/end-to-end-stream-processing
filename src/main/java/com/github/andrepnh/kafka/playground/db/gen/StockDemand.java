package com.github.andrepnh.kafka.playground.db.gen;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class StockDemand implements StockActivity {
  private static final int MAX_DEMAND_QUANTITY = 1000;

  private final int stockItemId;

  private final int warehouseId;

  private final int quantity;

  public StockDemand(
      int warehouseId, int stockItemId, int quantity) {
    checkArgument(quantity > 0);
    this.stockItemId = stockItemId;
    this.warehouseId = warehouseId;
    this.quantity = quantity;
  }

  public static StockDemand random(int warehouseId, int stockItemId) {
    return new StockDemand(warehouseId, stockItemId, Generator.positive(MAX_DEMAND_QUANTITY));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StockDemand that = (StockDemand) o;
    return stockItemId == that.stockItemId
        && warehouseId == that.warehouseId
        && quantity == that.quantity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(stockItemId, warehouseId, quantity);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("stockItemId", stockItemId)
        .add("warehouseId", warehouseId)
        .add("quantity", quantity)
        .toString();
  }

  @Override
  public int getStockItemId() {
    return stockItemId;
  }

  @Override
  public int getWarehouseId() {
    return warehouseId;
  }

  @Override
  public int getQuantity() {
    return quantity;
  }
}
