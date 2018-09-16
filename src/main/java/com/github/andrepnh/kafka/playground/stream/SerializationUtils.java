package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;

public class SerializationUtils {
  public static final ObjectMapper SHARED_MAPPER = new ObjectMapper()
      .registerModule(new ParameterNamesModule())
      .registerModule(new JavaTimeModule());

  public static <T> T deserialize(JsonNode node, Class<T> clazz) {
    try {
      return SHARED_MAPPER.treeToValue(node, clazz);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

  public static <T> T deserialize(JsonNode node, TypeReference<T> typeReference) {
    try {
      var json = SHARED_MAPPER.writeValueAsString(node);
      return SHARED_MAPPER.readValue(json, typeReference);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

}
