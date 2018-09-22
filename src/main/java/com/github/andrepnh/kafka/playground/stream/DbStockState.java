package com.github.andrepnh.kafka.playground.stream;

import com.github.andrepnh.kafka.playground.db.gen.StockState;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class DbStockState {
  private final int warehouseid;

  private final int stockitemid;

  private final int supply;

  private final int demand;

  private final int reserved;

  private final long lastupdate;

  public DbStockState(int warehouseid, int stockitemid, int supply, int demand, int reserved,
      long lastupdate) {
    this.warehouseid = warehouseid;
    this.stockitemid = stockitemid;
    this.supply = supply;
    this.demand = demand;
    this.reserved = reserved;
    this.lastupdate = lastupdate;
  }

  public StockState toStockState() {
    return new StockState(warehouseid, stockitemid, supply, demand, reserved,
        Instant.ofEpochMilli(lastupdate).atZone(ZoneOffset.UTC));
  }
}
