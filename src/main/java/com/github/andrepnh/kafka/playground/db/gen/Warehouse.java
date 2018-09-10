package com.github.andrepnh.kafka.playground.db.gen;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import org.eclipse.collections.api.tuple.Pair;

public class Warehouse {
  private static final int MAX_STORAGE_CAPACITY = 1000000;

  private final int id;

  private final String name;

  private final float latitude;

  private final float longitude;

  private final int storageCapacity;

  public Warehouse(int id, String name, int storageCapacity, float latitude, float longitude) {
    checkArgument(Math.abs(latitude) <= 90);
    checkArgument(Math.abs(longitude) <= 180);
    checkArgument(storageCapacity > 0);
    this.id = id;
    this.name = name;
    this.storageCapacity = storageCapacity;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public static Warehouse random(int idUpperBoundInclusive) {
    Pair<Float, Float> latitudeAndLongitude = Generator.randomLocation();
    return new Warehouse(
        Generator.positive(idUpperBoundInclusive),
        Generator.words(),
        Generator.positive(MAX_STORAGE_CAPACITY),
        latitudeAndLongitude.getOne(),
        latitudeAndLongitude.getTwo());
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
}
