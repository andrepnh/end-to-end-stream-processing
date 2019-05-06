package com.github.andrepnh.kafka.playground.stream;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public interface SimpleSerializer<T> extends Serializer<T> {
  @Override
  default void configure(Map<String, ?> configs, boolean isKey) {
    // noop
  }

  @Override
  default void close() {
    // noop
  }
}
