package com.github.andrepnh.kafka.playground.stream;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class WarehouseKey {
  private final int id;

  private final String name;

  private final float latitude;

  private final float longitude;

  public WarehouseKey(int id, String name, float latitude, float longitude) {
    this.id = id;
    this.name = name;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WarehouseKey that = (WarehouseKey) o;
    return id == that.id &&
        Float.compare(that.latitude, latitude) == 0 &&
        Float.compare(that.longitude, longitude) == 0 &&
        Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, latitude, longitude);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("name", name)
        .add("latitude", latitude)
        .add("longitude", longitude)
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
}
