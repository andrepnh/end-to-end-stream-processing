package com.github.andrepnh.kafka.playground.stream;

import com.github.andrepnh.kafka.playground.db.Warehouse;
import com.google.common.base.MoreObjects;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Objects;

public class DbWarehouse {
  private final int id;

  private final String name;

  private final float latitude;

  private final float longitude;

  private final int storagecapacity;

  private final long lastupdate;

  public DbWarehouse(int id, String name, int storagecapacity, float latitude, float longitude,
      long lastupdate) {
    this.id = id;
    this.name = name;
    this.storagecapacity = storagecapacity;
    this.latitude = latitude;
    this.longitude = longitude;
    this.lastupdate = lastupdate;
  }

  public Warehouse toWarehouse() {
    return new Warehouse(id, name, storagecapacity, latitude, longitude,
        Instant.ofEpochMilli(lastupdate).atZone(ZoneOffset.UTC));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DbWarehouse that = (DbWarehouse) o;
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
        .add("storagecapacity", storagecapacity)
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

  public int getStoragecapacity() {
    return storagecapacity;
  }
}
