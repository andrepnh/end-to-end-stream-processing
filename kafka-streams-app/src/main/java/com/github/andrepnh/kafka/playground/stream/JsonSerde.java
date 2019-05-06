package com.github.andrepnh.kafka.playground.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements SimpleSerde<T> {
  private final Serializer<T> serializer;

  private final Deserializer<T> deserializer;

  private JsonSerde(Class<T> clazz) {
    this.serializer = new JsonSerializer<>(SerializationUtils.SHARED_MAPPER);
    this.deserializer = new JsonDeserializer<>(SerializationUtils.SHARED_MAPPER, clazz);
  }

  private JsonSerde(TypeReference<T> typeReference) {
    this.serializer = new JsonSerializer<>(SerializationUtils.SHARED_MAPPER);
    this.deserializer = new JsonDeserializer<>(SerializationUtils.SHARED_MAPPER, typeReference);
  }

  public static <T> JsonSerde<T> of(Class<T> clazz) {
    return new JsonSerde<>(clazz);
  }

  public static <T> JsonSerde<T> of(TypeReference<T> typeRef) {
    return new JsonSerde<>(typeRef);
  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }

  private static class JsonSerializer<T> implements SimpleSerializer<T> {
    private final ObjectMapper mapper;

    public JsonSerializer(ObjectMapper mapper) {
      this.mapper = checkNotNull(mapper);
    }

    @Override
    public byte[] serialize(String topic, T data) {
      try {
        return mapper.writeValueAsBytes(data);
      } catch (JsonProcessingException e) {
        throw new SerializationException(e);
      }
    }

  }

  private static class JsonDeserializer<T> implements SimpleDeserializer<T> {
    private final ObjectMapper mapper;

    private final TypeReference<T> typeReference;

    private final Class<T> clazz;

    public JsonDeserializer(ObjectMapper mapper, Class<T> clazz) {
      this.mapper = mapper;
      this.clazz = checkNotNull(clazz);
      this.typeReference = null;
    }

    public JsonDeserializer(ObjectMapper mapper, TypeReference<T> typeReference) {
      this.mapper = mapper;
      this.typeReference = checkNotNull(typeReference);
      this.clazz = null;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
      try {
        if (typeReference != null) {
          return mapper.readValue(data, typeReference);
        } else {
          return mapper.readValue(data, clazz);
        }
      } catch (IOException e) {
        throw new SerializationException(e);
      }
    }

  }
}
