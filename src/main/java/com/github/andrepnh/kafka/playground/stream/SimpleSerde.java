package com.github.andrepnh.kafka.playground.stream;

import java.util.Map;
import org.apache.kafka.common.serialization.Serde;

public interface SimpleSerde<T> extends Serde<T> {
  @Override
  default void configure(Map<String, ?> configs, boolean isKey) {
    // noop
  }

  @Override
  default void close() {
    // noop
  }
}
