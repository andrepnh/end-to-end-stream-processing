package com.github.andrepnh.kafka.playground.db.gen;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class StorageNode {
  private final int id;

  private final String name;

  public StorageNode(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public static StorageNode random(int idUpperBoundInclusive) {
    return new StorageNode(Generator.positive(idUpperBoundInclusive), Generator.words());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StorageNode that = (StorageNode) o;
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
        .toString();
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }
}
