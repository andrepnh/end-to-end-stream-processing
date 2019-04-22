package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class WarehouseAllocation {
  private final String name;

  private final Location location;

  private final double allocation;

  private final AllocationThreshold threshold;

  private final ZonedDateTime lastUpdate;

  public WarehouseAllocation(String name, float latitude, float longitude, double allocation,
      AllocationThreshold threshold, ZonedDateTime lastUpdate) {
    checkNotNull(threshold);
    checkNotNull(lastUpdate);
    this.name = name;
    this.location = new Location(latitude, longitude);
    this.allocation = allocation;
    this.threshold = threshold;
    this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC);
  }

  public static WarehouseAllocation calculate(String name, float latitude, float longitude,
      int stockItems, int warehouseCapacity, ZonedDateTime lastUpdate) {
    var allocation = (double) stockItems / warehouseCapacity;
    if (allocation <= AllocationThreshold.LOW.getThreshold()) {
      return new WarehouseAllocation(name, latitude, longitude, allocation,
          AllocationThreshold.LOW, lastUpdate);
    } else if (allocation <= AllocationThreshold.NORMAL.getThreshold()) {
      return new WarehouseAllocation(name, latitude, longitude, allocation,
          AllocationThreshold.NORMAL, lastUpdate);
    } else {
      return new WarehouseAllocation(name, latitude, longitude, allocation,
          AllocationThreshold.HIGH, lastUpdate);
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
    return Double.compare(that.allocation, allocation) == 0 &&
        Objects.equals(name, that.name) &&
        Objects.equals(location, that.location) &&
        threshold == that.threshold &&
        Objects.equals(lastUpdate, that.lastUpdate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, location, allocation, threshold, lastUpdate);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("location", location)
        .add("allocation", allocation)
        .add("threshold", threshold)
        .add("lastUpdate", lastUpdate)
        .toString();
  }

  public double getAllocation() {
    return allocation;
  }

  public AllocationThreshold getThreshold() {
    return threshold;
  }

  public String getName() {
    return name;
  }

  public Location getLocation() {
    return location;
  }

  public ZonedDateTime getLastUpdate() {
    return lastUpdate;
  }

  public static class Location {
    private final float lat;

    private final float lon;

    public Location(float lat, float lon) {
      this.lat = lat;
      this.lon = lon;
    }

    public float getLat() {
      return lat;
    }

    public float getLon() {
      return lon;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Location location = (Location) o;
      return Float.compare(location.lat, lat) == 0 &&
          Float.compare(location.lon, lon) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lat, lon);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("lat", lat)
          .add("lon", lon)
          .toString();
    }
  }
}
