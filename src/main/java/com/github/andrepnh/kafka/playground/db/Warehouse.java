package com.github.andrepnh.kafka.playground.db;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Warehouse {
  private final int id;

  private final String name;

  private final int storageCapacity;

  private final float latitude;

  private final float longitude;

  private final ZonedDateTime lastUpdate;

  public Warehouse(int id, String name, int storageCapacity, float latitude, float longitude,
      ZonedDateTime lastUpdate) {
    checkArgument(Math.abs(latitude) <= 90);
    checkArgument(Math.abs(longitude) <= 180);
    checkArgument(storageCapacity > 0);
    checkArgument(!Strings.isNullOrEmpty(name));
    checkNotNull(lastUpdate);
    this.id = id;
    this.name = name;
    this.latitude = latitude;
    this.longitude = longitude;
    this.storageCapacity = storageCapacity;
    // Nanosecond precision is not available
    this.lastUpdate = lastUpdate.withZoneSameInstant(ZoneOffset.UTC).withNano(0);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Warehouse that = (Warehouse) o;
    return id == that.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("name", name)
        .add("latitude", latitude)
        .add("longitude", longitude)
        .add("storageCapacity", storageCapacity)
        .add("lastUpdate", lastUpdate)
        .toString();
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public float getLatitude() {
    return latitude;
  }

  public float getLongitude() {
    return longitude;
  }

  public int getStorageCapacity() {
    return storageCapacity;
  }

  public ZonedDateTime getLastUpdate() {
    return lastUpdate;
  }
}
