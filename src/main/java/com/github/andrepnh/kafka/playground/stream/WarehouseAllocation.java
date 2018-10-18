package com.github.andrepnh.kafka.playground.stream;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class WarehouseAllocation {
  private final String name;

  private final float latitude;

  private final float longitude;

  private final double allocation;

  private final AllocationThreshold threshold;

  public WarehouseAllocation(String name, float latitude, float longitude, double allocation,
      AllocationThreshold threshold) {
    this.name = name;
    this.latitude = latitude;
    this.longitude = longitude;
    this.allocation = allocation;
    this.threshold = threshold;
  }

  public static WarehouseAllocation calculate(String name, float latitude, float longitude,
      int stockItems, int warehouseCapacity) {
    var allocation = (double) stockItems / warehouseCapacity;
    if (allocation <= AllocationThreshold.LOW.getThreshold()) {
      return new WarehouseAllocation(name, latitude, longitude, allocation, AllocationThreshold.LOW);
    } else if (allocation <= AllocationThreshold.NORMAL.getThreshold()) {
      return new WarehouseAllocation(name, latitude, longitude, allocation, AllocationThreshold.NORMAL);
    } else {
      return new WarehouseAllocation(name, latitude, longitude, allocation, AllocationThreshold.HIGH);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WarehouseAllocation that = (WarehouseAllocation) o;
    return allocation == that.allocation && threshold == that.threshold;
  }

  @Override
  public int hashCode() {
    return Objects.hash(allocation, threshold);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("allocation", allocation)
        .add("threshold", threshold)
        .toString();
  }

  public double getAllocation() {
    return allocation;
  }

  public AllocationThreshold getThreshold() {
    return threshold;
  }
}
