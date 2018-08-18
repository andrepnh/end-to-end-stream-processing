package com.github.andrepnh;

import com.github.andrepnh.kafka.playground.KafkaProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.multimap.list.ImmutableListMultimap;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.list.primitive.IntInterval;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ConsumerAndProducerApiTest {

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
            // and always use an unique topic name, which means once the consumer group is spun up
            // it has no offset. When this happens by default the group will be positioned after the
            // last record, which means it'll skip the record we just sent.
            // By changing auto.offset.reset to "earliest" the group will consumeUntilNoRecordsFound
            // records from the
            // very beginning of the topic-partition.
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .build();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Lists.newArrayList(topic));
      // Using a high timeout since we are not consuming on loop and the topic itself was just
      // created, which could cause UnknownTopicOrPartitionException since leader election could be
      // in progress.
      // https://stackoverflow.com/questions/44514923/reproducing-unknowntopicorpartitionexception-this-server-does-not-host-this-top
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
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
    KafkaProperties.createTopic(topic, partitions, 1).all().get();
    final int keys = 100, messagesPerKey = 100;
    try (var producer =
        new KafkaProducer<>(KafkaProperties.newDefaultProducerProperties().build())) {
      for (int i = 0; i < keys; i++) {
        for (int j = 0; j < messagesPerKey; j++) {
          producer.send(new ProducerRecord<>(topic, "key_" + i, "value_" + j));
        }
      }
    }

    var executorService = Executors.newFixedThreadPool(partitions);
    final var consumerProps =
        KafkaProperties.newDefaultConsumerProperties()
            .put(ConsumerConfig.GROUP_ID_CONFIG, topic + "_group")
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .build();
    List<Future<ImmutableList<ConsumerResults>>> futures =
        executorService.invokeAll(
            Collections.nCopies(
                partitions, () -> consumeUntilNoRecordsFound(topic, consumerProps, partitions, 50)));
    executorService.shutdown();
    boolean finished = executorService.awaitTermination(5, TimeUnit.SECONDS);
    executorService.shutdownNow();
    assertTrue("Timeout elapsed before all messages were consumed", finished);
    List<ImmutableList<ConsumerResults>> results = getFutures(futures);
    int totalRecordsConsumed =
        results.stream().flatMap(ImmutableList::stream).mapToInt(ConsumerResults::getRecords).sum();
    assertEquals(keys * messagesPerKey, totalRecordsConsumed);
    printRecordsConsumedCount(results);
  }

  @Test
  public void singleConsumerPerGroupAndMultipleGroups()
      throws ExecutionException, InterruptedException {
    final String topic = KafkaProperties.uniqueTopic("single-consumer-in-multiple-groups");
    final int partitions = KafkaProperties.BROKERS, consumer_groups = KafkaProperties.BROKERS;
    KafkaProperties.createTopic(topic, partitions, 1).all().get();
    final int records = 10000;
    try (var producer =
        new KafkaProducer<>(KafkaProperties.newDefaultProducerProperties().build())) {
      for (int i = 0; i < records; i++) {
        producer.send(new ProducerRecord<>(topic, "key_" + i, "value_" + i));
      }
    }

    var executorService = Executors.newFixedThreadPool(consumer_groups);
    ImmutableList<Callable<ImmutableList<ConsumerResults>>> consumers =
        IntInterval.zeroTo(consumer_groups)
            .collect(
                i ->
                    KafkaProperties.newDefaultConsumerProperties()
                        .put(ConsumerConfig.GROUP_ID_CONFIG, topic + "_group" + i)
                        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .build())
            .collect(props -> () -> consumeUntilNoRecordsFound(topic, props, partitions, 10));
    List<Future<ImmutableList<ConsumerResults>>> futures =
        executorService.invokeAll(consumers.toList());
    executorService.shutdown();
    boolean finished = executorService.awaitTermination(15, TimeUnit.SECONDS);
    executorService.shutdownNow();
    assertTrue("Timeout elapsed before all messages were consumed", finished);
    List<ImmutableList<ConsumerResults>> results = getFutures(futures);
    ImmutableListMultimap<String, ConsumerResults> resultsByGroup =
        groupByConsumerGroup(getFutures(futures));
    resultsByGroup.forEachKeyMultiValues(
        (group, groupResults) -> {
          var groupResultsList = Lists.newArrayList(groupResults);
          assertEquals(partitions, groupResultsList.size());
          assertEquals(
              records, groupResultsList.stream().mapToInt(ConsumerResults::getRecords).sum());
        });
    printRecordsConsumedCountGroupedByGroup(resultsByGroup);
  }

  @Test
  public void cannotHaveMoreConsumersThanPartitions()
      throws ExecutionException, InterruptedException {
    final String topic = KafkaProperties.uniqueTopic("more-groups-than-partitions");
    final int partitions = 2, consumerQty = partitions + 1;
    KafkaProperties.createTopic(topic, partitions, 1).all().get();
    final int records = 1;
    try (var producer =
        new KafkaProducer<>(KafkaProperties.newDefaultProducerProperties().build())) {
      for (int i = 0; i < records; i++) {
        producer.send(new ProducerRecord<>(topic, "key_" + i, "value_" + i));
      }
    }

    var executorService = Executors.newFixedThreadPool(consumerQty);
    ImmutableList<Callable<ImmutableList<ConsumerResults>>> consumers =
        IntInterval.zeroTo(consumerQty)
            .collect(i -> KafkaProperties.newDefaultConsumerProperties()
                .put(ConsumerConfig.GROUP_ID_CONFIG, topic + "_group")
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .build())
            .collect(props -> () -> consumeUntilNoRecordsFound(topic, props, partitions, 20));
    List<Future<ImmutableList<ConsumerResults>>> futures =
        executorService.invokeAll(consumers.toList());
    executorService.shutdown();
    boolean finished = executorService.awaitTermination(15, TimeUnit.SECONDS);
    executorService.shutdownNow();
    assertTrue("Timeout elapsed before all messages were consumed", finished);
    ImmutableList<ConsumerResults> consumersWithRecords =
        org.eclipse.collections.impl.factory.Lists.immutable
            .ofAll(getFutures(futures))
            .flatCollect(Functions.identity())
            .select(consumerResults -> consumerResults.getRecords() > 0);
    assertNotEquals(consumerQty, consumersWithRecords.size());
    assertEquals(records, consumersWithRecords.size());
    consumersWithRecords.forEach(System.out::println);
  }

  private <T> List<T> getFutures(List<Future<T>> futures) {
    return futures
        .stream()
        .map(future -> {
            try {
              return future.get();
            } catch (InterruptedException | ExecutionException e) {
              throw new IllegalStateException(e);
            }
        }).collect(Collectors.toList());
  }

  private ImmutableList<ConsumerResults> consumeUntilNoRecordsFound(
      String topic, ImmutableMap<String, Object> consumerProps, int partitions, int noRecordsLimit) {
    int noRecordsFoundCount = 0;
    var consumerGroup = consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG).toString();
    var results = IntInterval.zeroTo(partitions)
          .collect(partition -> new ConsumerResults(
              topic, consumerGroup, Thread.currentThread().getName(), partition));
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Lists.newArrayList(topic));
      while (noRecordsFoundCount < noRecordsLimit && !Thread.interrupted()) {
        var recordsFound = consumer.poll(Duration.ofMillis(50));
        for (ConsumerRecord<String, String> record : recordsFound) {
          results.get(record.partition()).inc();
        }
        noRecordsFoundCount += recordsFound.isEmpty() ? 1 : 0;
        consumer.commitSync();
      }
    }
    return results;
  }

  private ImmutableListMultimap<String, ConsumerResults> groupByConsumerGroup(
      List<ImmutableList<ConsumerResults>> results) {
    return org.eclipse.collections.impl.factory.Lists.immutable
        .ofAll(results)
        .flatCollect(Functions.identity())
        .select(result -> result.getRecords() > 0)
        .groupBy(ConsumerResults::getConsumerGroup);
  }

  private void printRecordsConsumedCountGroupedByGroup(
      ImmutableListMultimap<String, ConsumerResults> resultsByGroup) {
    resultsByGroup.forEachKeyMultiValues(
        (group, groupResults) -> {
          var groupResultsList = Lists.newArrayList(groupResults);
          System.out.format("Group %s:\n", group);
          groupResultsList.sort(Comparator.naturalOrder());
          groupResultsList.forEach(
              result -> {
                System.out.format(
                    "  Partition %d:\n    Consumer %s: %d records\n",
                    result.getPartition(), result.getConsumer(), result.getRecords());
              });
        });
  }

  private void printRecordsConsumedCount(List<ImmutableList<ConsumerResults>> results) {
    org.eclipse.collections.impl.factory.Lists.immutable
        .ofAll(results)
        .flatCollect(Functions.identity())
        .select(result -> result.getRecords() > 0)
        .toSortedList()
        .forEach(System.out::println);
  }

  private static class ConsumerResults implements Comparable<ConsumerResults> {
    private static final Comparator<ConsumerResults> COMPARATOR =
        Comparator.comparing(ConsumerResults::getTopic)
            .thenComparing(ConsumerResults::getPartition)
            .thenComparing(ConsumerResults::getConsumerGroup)
            .thenComparing(ConsumerResults::getConsumer)
            .thenComparing(ConsumerResults::getRecords);

    private final String topic;

    private final String consumerGroup;

    private final String consumer;

    private final int partition;

    private int records;

    public ConsumerResults(String topic, String consumerGroup, String consumer, int partition) {
      this.topic = topic;
      this.consumerGroup = consumerGroup;
      this.consumer = consumer;
      this.partition = partition;
    }

    public ConsumerResults inc() {
      records++;
      return this;
    }

    @Override
    public int compareTo(ConsumerResults o) {
      return COMPARATOR.compare(this, o);
    }

    @Override
    public String toString() {
      return "Topic: " + topic + "; partition: " + partition + ":\n"
          + "  Consumer group: " + consumerGroup + ":\n"
          + "    Consume " + consumer + " " + records + " consumed";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ConsumerResults that = (ConsumerResults) o;
      return partition == that.partition
          && records == that.records
          && Objects.equals(topic, that.topic)
          && Objects.equals(consumerGroup, that.consumerGroup)
          && Objects.equals(consumer, that.consumer);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topic, consumerGroup, consumer, partition, records);
    }

    public String getTopic() {
      return topic;
    }

    public String getConsumerGroup() {
      return consumerGroup;
    }

    public String getConsumer() {
      return consumer;
    }

    public int getPartition() {
      return partition;
    }

    public int getRecords() {
      return records;
    }
  }
}
