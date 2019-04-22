package com.github.andrepnh.kafka.playground;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.NewTopic;

public final class TopicCreator {

  /**
   * Same as {@code TopicCreator.create(topicNamePrefix, 1);}
   *
   * @see #create(String, int)
   */
  public static TopicProperties create(String topicNamePrefix) {
    return create(topicNamePrefix, 1);
  }

  /**
   * Same as {@code TopicCreator.create(topicNamePrefix, partitions, 1);}
   *
   * @see #create(String, int, int)
   */
  public static TopicProperties create(String topicNamePrefix, int partitions) {
    return create(topicNamePrefix, partitions, 1);
  }

  /**
   * Create a unique topic named after the given prefix, using the parameterized partitions and
   * replication factor.
   * @return the created topic's properties
   */
  public static TopicProperties create(String topicNamePrefix, int partitions, int replicationFactor) {
    try {
      var name = uniqueTopicName(topicNamePrefix);
      DefaultAdminClient.INSTANCE
          .createTopics(
              Lists.newArrayList(new NewTopic(name, partitions, (short) replicationFactor)))
          .all()
          .get();
      return new TopicProperties(name, partitions, replicationFactor);
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String uniqueTopicName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  public static class TopicProperties {
    private final String name;
    private final int partitions;
    private final int replicationFactor;

    public TopicProperties(String name, int partitions, int replicationFactor) {
      this.name = name;
      this.partitions = partitions;
      this.replicationFactor = replicationFactor;
    }

    public String getName() {
      return name;
    }

    public int getPartitions() {
      return partitions;
    }

    public int getReplicationFactor() {
      return replicationFactor;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TopicProperties that = (TopicProperties) o;
      return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", name)
          .add("partitions", partitions)
          .add("replicationFactor", replicationFactor)
          .toString();
    }
  }

  private TopicCreator() { }
}
