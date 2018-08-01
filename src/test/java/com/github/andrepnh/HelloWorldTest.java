package com.github.andrepnh;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HelloWorldTest {


//    @Test
    public void foo() throws ExecutionException, InterruptedException {
        Properties producerProps = new Properties();
        String bootstrapServer = System.getProperty("kafka_1.host") + ":" + System.getProperty("kafka_1.tcp.9092");
        producerProps.put("bootstrap.servers", bootstrapServer);
        try (Producer<String, String> producer = new KafkaProducer<String, String>(producerProps, new StringSerializer(), new StringSerializer())) {
            Future<RecordMetadata> metadata = producer.send(new ProducerRecord<>("some-topic", "key", "hello world"));
            RecordMetadata recordMetadata = metadata.get();
        }

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServer);
        consumerProps.put("group.id", "key");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(Arrays.asList("some-topic"));
            ConsumerRecords<String, String> records = consumer.poll(1);
            boolean foundRecord = false;
            for (ConsumerRecord<String, String> record : records) {
                foundRecord = true;
                assertEquals("hello world", record.value());
            }
            assertTrue(foundRecord);
            consumer.commitSync();
        }
    }
}
