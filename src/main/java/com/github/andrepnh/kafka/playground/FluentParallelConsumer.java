package com.github.andrepnh.kafka.playground;

import static com.google.common.base.Preconditions.checkState;

import com.github.andrepnh.kafka.playground.TopicCreator.TopicProperties;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.list.primitive.IntInterval;

public class FluentParallelConsumer {
  private BiConsumer<Integer, ImmutableMap.Builder<String, Object>> propertiesExtensionChain =
      (i, builder) -> {};

  private final int consumerQty;

  public FluentParallelConsumer(int consumerQty) {
    this.consumerQty = consumerQty;
  }

  public FluentParallelConsumer extendingDefaultProperties(
      BiConsumer<Integer, ImmutableMap.Builder<String, Object>> propertiesExtension) {
    propertiesExtensionChain = propertiesExtensionChain.andThen(propertiesExtension);
    return this;
  }

  public FluentParallelConsumer resetOffsetToEarliest() {
    propertiesExtensionChain = propertiesExtensionChain
        .andThen((i, builder) -> builder.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    return this;
  }

  public FluentParallelConsumer withTopicBasedGroupName(String topic) {
    propertiesExtensionChain = propertiesExtensionChain
        .andThen((i, builder) -> builder.put(ConsumerConfig.GROUP_ID_CONFIG, topic + "_group"));
    return this;
  }

  public FluentParallelConsumer withGroupName(IntFunction<String> groupNameSupplier) {
    propertiesExtensionChain =
        propertiesExtensionChain.andThen(
            (consumerNumber, builder) ->
                builder.put(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    groupNameSupplier.apply(consumerNumber)));
    return this;
  }

  public ImmutableList<ConsumerResults> consumeUntilNoRecordsFound(
      TopicProperties topic, int noRecordsLimit, Duration timeout) {
    ImmutableList<Callable<ImmutableList<ConsumerResults>>> consumers = IntInterval
        .zeroTo(consumerQty)
        .collect(consumerNumber -> {
          var propertiesBuilder = ClusterProperties.newDefaultConsumerProperties();
          propertiesExtensionChain.accept(consumerNumber, propertiesBuilder);
          return propertiesBuilder.build();
        }).collect(props -> () -> consumeUntilNoRecordsFound(topic, props, noRecordsLimit));
    ImmutableList<ImmutableList<ConsumerResults>> resultsByConsumer = execute(consumers, timeout);
    return resultsByConsumer.flatCollect(Functions.identity());
  }

  private ImmutableList<ImmutableList<ConsumerResults>> execute(
      ImmutableList<Callable<ImmutableList<ConsumerResults>>> consumers,
      Duration timeout) {
    try {
      var executorService = Executors.newFixedThreadPool(consumerQty);
      List<Future<ImmutableList<ConsumerResults>>> futures =
          executorService.invokeAll(consumers.toList());
      executorService.shutdown();
      boolean finished = executorService.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
      executorService.shutdownNow();
      checkState(finished,
          "Consumers not finished after waiting for this duration: %s",
          timeout);
      ImmutableList<ImmutableList<ConsumerResults>> results = getFutures(futures);
      return results;
    } catch (InterruptedException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private <T> ImmutableList<T> getFutures(List<Future<T>> futures) {
    return org.eclipse.collections.impl.factory.Lists.immutable
        .ofAll(futures)
        .collect(future -> {
          try {
            return future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
          }
        });
  }

  private ImmutableList<ConsumerResults> consumeUntilNoRecordsFound(
      TopicProperties topic, ImmutableMap<String, Object> consumerProps, int noRecordsLimit) {
    int noRecordsFoundCount = 0;
    var consumerGroup = consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG).toString();
    var results = IntInterval.zeroTo(topic.getPartitions())
        .collect(partition -> new ConsumerResults(
            topic.getName(), consumerGroup, Thread.currentThread().getName(), partition));
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Lists.newArrayList(topic.getName()));
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

  public static class ConsumerResults implements Comparable<ConsumerResults> {
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
      return "Topic: "
          + topic
          + "; partition: "
          + partition
          + ":\n"
          + "  Consumer group: "
          + consumerGroup
          + ":\n"
          + "    Consume "
          + consumer
          + " "
          + records
          + " consumed";
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
