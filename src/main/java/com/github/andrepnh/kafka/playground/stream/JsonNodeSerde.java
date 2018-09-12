package com.github.andrepnh.kafka.playground.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonNodeSerde implements Serde<JsonNode> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // noop
  }

  @Override
  public void close() {
    // noop
  }

  @Override
  public Serializer<JsonNode> serializer() {
    return new JsonNodeSerializer();
  }

  @Override
  public Deserializer<JsonNode> deserializer() {
    return new JsonNodeDeserializer();
  }

  private static class JsonNodeSerializer implements Serializer<JsonNode> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // noop
    }

    @Override
    public byte[] serialize(String topic, JsonNode data) {
      try {
        return MAPPER.writeValueAsBytes(data);
      } catch (IOException e) {
        throw new SerializationException(e);
      }
    }

    @Override
    public void close() {
      // noop
    }
  }

  private static class JsonNodeDeserializer implements Deserializer<JsonNode> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // noop
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
      try {
        if (data == null || data.length == 0) {
          return null;
        }
        return MAPPER.readTree(data);
      } catch (IOException e) {
        throw new SerializationException(e);
      }
    }

    @Override
    public void close() {
      // noop
    }
  }

}
