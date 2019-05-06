package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class JsonNodeSerde implements SimpleSerde<JsonNode> {
  @Override
  public Serializer<JsonNode> serializer() {
    return new JsonNodeSerializer(SerializationUtils.SHARED_MAPPER);
  }

  @Override
  public Deserializer<JsonNode> deserializer() {
    return new JsonNodeDeserializer(SerializationUtils.SHARED_MAPPER);
  }

  private static class JsonNodeSerializer implements SimpleSerializer<JsonNode> {
    private final ObjectMapper mapper;

    public JsonNodeSerializer(ObjectMapper mapper) {
      this.mapper = checkNotNull(mapper);
    }

    @Override
    public byte[] serialize(String topic, JsonNode data) {
      try {
        return mapper.writeValueAsBytes(data);
      } catch (IOException e) {
        throw new SerializationException(e);
      }
    }
  }

  private static class JsonNodeDeserializer implements SimpleDeserializer<JsonNode> {
    private final ObjectMapper mapper;

    public JsonNodeDeserializer(ObjectMapper mapper) {
      this.mapper = checkNotNull(mapper);
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
      try {
        if (data == null || data.length == 0) {
          return null;
        }
        return mapper.readTree(data);
      } catch (IOException e) {
        throw new SerializationException(e);
      }
    }
  }

}
