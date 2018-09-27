package com.github.andrepnh.kafka.playground.stream;

public enum AllocationThreshold {
  LOW(0.25), NORMAL(0.75), HIGH(1.0);

  private final double threshold;

  AllocationThreshold(double threshold) {
    this.threshold = threshold;
  }

  public double getThreshold() {
    return threshold;
  }
}
