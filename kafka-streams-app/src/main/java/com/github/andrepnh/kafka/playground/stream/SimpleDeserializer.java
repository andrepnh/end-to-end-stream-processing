package com.github.andrepnh.kafka.playground.stream;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public interface SimpleDeserializer<T> extends Deserializer<T> {
  @Override
  default void configure(Map<String, ?> configs, boolean isKey) {
    // noop
  }

  @Override
  default void close() {
    // noop
  }
}
