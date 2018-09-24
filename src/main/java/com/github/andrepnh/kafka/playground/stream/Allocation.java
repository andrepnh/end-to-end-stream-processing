package com.github.andrepnh.kafka.playground.stream;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class Allocation {
  private final double softAllocation;

  private final double hardAllocation;

  public Allocation(double softAllocation, double hardAllocation) {
    this.softAllocation = softAllocation;
    this.hardAllocation = hardAllocation;
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
    return softAllocation == that.softAllocation &&
        hardAllocation == that.hardAllocation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(softAllocation, hardAllocation);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("softAllocation", softAllocation)
        .add("hardAllocation", hardAllocation)
        .toString();
  }

  public double getSoftAllocation() {
    return softAllocation;
  }

  public double getHardAllocation() {
    return hardAllocation;
  }
}
