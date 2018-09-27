package com.github.andrepnh.kafka.playground.stream;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class Allocation {
  private final double allocation;

  private final AllocationThreshold threshold;

  public Allocation(double allocation, AllocationThreshold threshold) {
    this.allocation = allocation;
    this.threshold = threshold;
  }

  public static Allocation calculate(int stockItems, int warehouseCapacity) {
    var allocation = (double) stockItems / warehouseCapacity;
    if (allocation <= AllocationThreshold.LOW.getThreshold()) {
      return new Allocation(allocation, AllocationThreshold.LOW);
    } else if (allocation <= AllocationThreshold.NORMAL.getThreshold()) {
      return new Allocation(allocation, AllocationThreshold.NORMAL);
    } else {
      return new Allocation(allocation, AllocationThreshold.HIGH);
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
    Allocation that = (Allocation) o;
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
