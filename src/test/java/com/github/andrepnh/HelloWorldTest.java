package com.github.andrepnh;

import com.github.andrepnh.kafka.playground.KafkaProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.Tables;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HelloWorldTest {

  @Test
  public void singleProducerAndConsumer() throws ExecutionException, InterruptedException {
    final String topic = KafkaProperties.uniqueTopic("simple-consumer-producer");
    var producerProps = KafkaProperties.newDefaultProducerProperties().build();
    try (var producer = new KafkaProducer<>(producerProps)) {
      producer.send(new ProducerRecord<>(topic, "whatever", "hello world")).get();
    }

    var consumerProps =
        KafkaProperties.newDefaultConsumerProperties()
            .put(ConsumerConfig.GROUP_ID_CONFIG, topic + "_group")
            // The following configuration is key for this test. We have auto topic creation enabled
            // and
            // always use an unique topic name, which means once the consumer group is spun up it
            // has
            // no offset. When this happens by default the group will be positioned after the last
            // record,
            // which means it'll skip the record we just sent.
            // By changing auto.offset.reset to "earliest" the group will consume records from the
            // very
            // beginning of the topic-partition.
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .build();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Lists.newArrayList(topic));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
      var record = Iterables.getOnlyElement(records);
      assertEquals("hello world", record.value());
      System.out.format(
          "Found record at partition %d, offset %d\n", record.partition(), record.offset());
      consumer.commitSync();
      // When using consumer groups each member periodically send heartbeats to the brokers
      // (see heartbeat.interval.ms consumer config). If after session.timeout.ms (another consumer
      // config) Kafka doesn't hear from then consumer, it'll be removed from its group triggering
      // partition rebalance.
      // So we actually don't need to unsubscribe after we're done consuming, at least not when
      // using consumer groups
      consumer.unsubscribe();
    }
  }

  @Test
  public void multipleConsumersInASingleGroup() throws ExecutionException, InterruptedException {
    final String topic = KafkaProperties.uniqueTopic("multiple-consumers-single-group");
    final int partitions = KafkaProperties.BROKERS;
    KafkaProperties.createTopic(topic, partitions, (short) 1).all().get();
    final int keys = 100, messagesPerKey = 100, totalMessages = keys * messagesPerKey;
    try (var producer = new KafkaProducer<>(KafkaProperties.newDefaultProducerProperties().build())) {
      for (int i = 0; i < keys; i++) {
        for (int j = 0; j < messagesPerKey; j++) {
          producer.send(new ProducerRecord<>(topic, "key_" + i, "value_" + j));
        }
      }
    }

    var executorService = Executors.newFixedThreadPool(partitions);
    var recordsLeft = new AtomicInteger(totalMessages);
    final var consumerProps = KafkaProperties.newDefaultConsumerProperties()
        .put(ConsumerConfig.GROUP_ID_CONFIG, topic + "_group")
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .build();
    List<Future<ConsumerRecordsCounter>> futures = executorService.invokeAll(
        Collections.nCopies(
            partitions,
            () -> consume(topic, consumerProps, recordsLeft, partitions)));
    executorService.shutdown();
    boolean finished = executorService.awaitTermination(5, TimeUnit.SECONDS);
    executorService.shutdownNow();
    assertTrue("Timeout elapsed before all messages were consumed", finished);
    printRecordsConsumedByThreadAndPartition(futures);
  }

  private ConsumerRecordsCounter consume(String topic, ImmutableMap<String, Object> consumerProps,
      AtomicInteger recordsLeft, int partitions) {
    var recordCounter = new ConsumerRecordsCounter(Thread.currentThread().getName(), partitions);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Lists.newArrayList(topic));
      while (recordsLeft.get() > 0 && !Thread.interrupted()) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(50))) {
          recordsLeft.decrementAndGet();
          recordCounter.recordConsumed(record.partition());
        }
        consumer.commitSync();
      }
    }
    return recordCounter;
  }

  private void printRecordsConsumedByThreadAndPartition(List<Future<ConsumerRecordsCounter>> futures) {
    org.eclipse.collections.impl.factory.Lists.immutable
        .ofAll(futures)
        .collect(future -> {
          try {
            return future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
          }
        }).flatCollect(counter -> counter
            .getCounterByPartition()
            .collectWithIndex((count, partition) ->
                Tables.immutableCell(counter.getConsumer(), partition, count)))
        .select(cell -> cell.getValue() > 0)
        .toSortedList(byCellRowAndThenByColumn())
        .forEach(cell ->
            System.out.printf(
                "Thread %s, partition %d: %d records consumed\n",
                cell.getRowKey(), cell.getColumnKey(), cell.getValue()));
  }

  private Comparator<Cell<String, Integer, ?>> byCellRowAndThenByColumn() {
    return Comparator.<Cell<String, Integer, ?>, String>comparing(Cell::getRowKey)
        .thenComparing(Cell::getColumnKey);
  }

  private static class ConsumerRecordsCounter {
    private final String consumer;

    private final MutableIntList counterByPartition;

    public ConsumerRecordsCounter(String consumer, int partitions) {
      this.consumer = consumer;
      this.counterByPartition = IntLists.mutable.of(new int[partitions]);
    }

    public ConsumerRecordsCounter recordConsumed(int partition) {
      counterByPartition.set(partition, counterByPartition.get(partition) + 1);
      return this;
    }

    public String getConsumer() {
      return consumer;
    }

    public IntList getCounterByPartition() {
      return counterByPartition;
    }
  }
}
