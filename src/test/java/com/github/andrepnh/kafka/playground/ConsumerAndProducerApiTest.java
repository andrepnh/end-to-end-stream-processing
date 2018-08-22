package com.github.andrepnh.kafka.playground;

import com.github.andrepnh.kafka.playground.FluentParallelConsumer.ConsumerResults;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.multimap.list.ImmutableListMultimap;
import org.eclipse.collections.impl.block.factory.Functions;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class ConsumerAndProducerApiTest {

  @Test
  public void singleProducerAndConsumer() throws ExecutionException, InterruptedException {
    final String topic = TopicCreator.uniqueTopicName("simple-consumer-producer");
    var producerProps = ProducerFacade.newDefaultProducerProperties().build();
    try (var producer = new KafkaProducer<>(producerProps)) {
      producer.send(new ProducerRecord<>(topic, "whatever", "hello world")).get();
    }

    var consumerProps =
        ClusterProperties.newDefaultConsumerProperties()
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
  public void multipleConsumersInASingleGroup() {
    final var topic = TopicCreator.create(
        "multiple-consumers-single-group", ClusterProperties.BROKERS);
    final int records = 1000;
    ProducerFacade.produce(topic.getName(), records);

    ImmutableList<ConsumerResults> results = new FluentParallelConsumer(topic.getPartitions())
        .resetOffsetToEarliest()
        .withTopicBasedGroupName(topic.getName())
        .consumeUntilNoRecordsFound(topic, 50, Duration.ofSeconds(5));
    long totalRecordsConsumed = results.collectInt(ConsumerResults::getRecords).sum();
    assertEquals(records, totalRecordsConsumed);
    printRecordsConsumedCount(results);
  }

  @Test
  public void singleConsumerPerGroupAndMultipleGroups() {
    final var topic = TopicCreator.create(
        "single-consumer-in-multiple-groups", ClusterProperties.BROKERS);
    final int records = 1000, consumerGroups = topic.getPartitions();
    ProducerFacade.produce(topic.getName(), records);

    ImmutableListMultimap<String, ConsumerResults> resultsByGroup =
        new FluentParallelConsumer(consumerGroups)
            .resetOffsetToEarliest()
            .withGroupName(groupNumber -> topic.getName() + "_group" + groupNumber)
            .consumeUntilNoRecordsFound(topic, 20, Duration.ofSeconds(15))
            .groupBy(ConsumerResults::getConsumerGroup);
    resultsByGroup.forEachKeyMultiValues(
        (group, results) -> {
          var nonEmptyResults = org.eclipse.collections.impl.factory.Lists.immutable
              .ofAll(results)
              .select(result -> result.getRecords() > 0);
          assertEquals(records, nonEmptyResults.collectInt(ConsumerResults::getRecords).sum());
        });
    printRecordsConsumedCountGroupedByGroup(resultsByGroup);
  }

  @Test
  public void recordsAreBalancedOnlyUpToAsManyConsumersAsPartitions() {
    final var topic = TopicCreator.create("more-groups-than-partitions", 2);
    final int records = 100, consumerQty = topic.getPartitions() + 10;
    ProducerFacade.produce(topic.getName(), records);

    ImmutableList<ConsumerResults> consumersWithRecords = new FluentParallelConsumer(consumerQty)
        .resetOffsetToEarliest()
        .withTopicBasedGroupName(topic.getName())
        .consumeRecords(topic, records, Duration.ofSeconds(5))
        .select(results -> results.getRecords() > 0);
    assertEquals(topic.getPartitions(), consumersWithRecords.size());
    consumersWithRecords.forEach(System.out::println);
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

  private void printRecordsConsumedCount(ImmutableList<ConsumerResults> results) {
    results.select(result -> result.getRecords() > 0)
        .toSortedList()
        .forEach(System.out::println);
  }


}
