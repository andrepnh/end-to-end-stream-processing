package com.github.andrepnh.kafka.playground.db;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class StockItem {
  private final int id;

  private final String description;

  public StockItem(int id, String description) {
    this.id = id;
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StockItem stockItem = (StockItem) o;
    return id == stockItem.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("description", description)
        .toString();
  }

  public int getId() {
    return id;
  }

  public String getDescription() {
    return description;
  }
}
