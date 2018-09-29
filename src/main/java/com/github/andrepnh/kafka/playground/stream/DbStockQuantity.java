package com.github.andrepnh.kafka.playground.stream;

import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import java.time.Instant;
import java.time.ZoneOffset;

public class DbStockQuantity {
  private final int warehouseid;

  private final int stockitemid;

  private final int quantity;

  private final long lastupdate;

  public DbStockQuantity(int warehouseid, int stockitemid, int quantity, long lastupdate) {
    this.warehouseid = warehouseid;
    this.stockitemid = stockitemid;
    this.quantity = quantity;
    this.lastupdate = lastupdate;
  }

  public StockQuantity toStockQuantity() {
    return new StockQuantity(warehouseid, stockitemid, quantity,
        Instant.ofEpochMilli(lastupdate).atZone(ZoneOffset.UTC));
  }
}
