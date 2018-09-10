package com.github.andrepnh.kafka.playground.db.gen;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class StockSupply implements StockActivity {
  private static final int MAX_SUPPLY_QUANTITY = 2000;

  private final int stockItemId;

  private final int warehouseId;

  private final int quantity;

  public StockSupply(
      int warehouseId, int stockItemId, int quantity) {
    checkArgument(quantity > 0);
    this.stockItemId = stockItemId;
    this.warehouseId = warehouseId;
    this.quantity = quantity;
  }

  public static StockSupply random(int warehouseId, int stockItemId) {
    return new StockSupply(warehouseId, stockItemId, Generator.positive(MAX_SUPPLY_QUANTITY));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StockSupply that = (StockSupply) o;
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
