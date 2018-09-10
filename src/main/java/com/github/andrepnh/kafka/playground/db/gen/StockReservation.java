package com.github.andrepnh.kafka.playground.db.gen;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class StockReservation implements StockActivity {
  private static final int MAX_RESERVATION_QUANTITY = 1500;

  private final int stockItemId;

  private final int warehouseId;

  private final int quantity;

  public StockReservation(
      int warehouseId, int stockItemId, int quantity) {
    checkArgument(quantity > 0);
    this.stockItemId = stockItemId;
    this.warehouseId = warehouseId;
    this.quantity = quantity;
  }

  public static StockReservation random(int warehouseId, int stockItemId) {
    return new StockReservation(warehouseId, stockItemId, Generator.positive(MAX_RESERVATION_QUANTITY));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StockReservation that = (StockReservation) o;
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
