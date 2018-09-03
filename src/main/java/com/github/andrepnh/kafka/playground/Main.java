package com.github.andrepnh.kafka.playground;

import static com.google.common.base.Preconditions.checkState;

import com.github.andrepnh.kafka.playground.db.gen.Generator;
import com.github.andrepnh.kafka.playground.db.gen.StockItem;
import com.github.andrepnh.kafka.playground.db.gen.StorageNode;
import com.github.andrepnh.kafka.playground.db.gen.StorageNodeActivity;
import com.google.common.collect.Streams;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.collections.impl.factory.Lists;

public class Main {
  private static final String INSERT_NODE = "INSERT INTO StorageNode(id, name) VALUES (?, ?) ON CONFLICT (id) DO NOTHING";

  private static final String INSERT_ITEM = "INSERT INTO StockItem(id, description) VALUES (?, ?) ON CONFLICT (id) DO NOTHING";

  private static final String INSERT_ACTIVITY = "INSERT INTO StorageNodeActivity"
      + "(storageNodeId, stockItemId, moment, quantity) VALUES (?, ?, ?, ?)";

  public static void main(String[] args)
      throws ClassNotFoundException, InterruptedException {
    var maxNodes = getProperty("max.storage.nodes", 3000, Integer::parseInt);
    var maxItems = getProperty("max.stock.items", 10000000, Integer::parseInt);
    var activityRecords = getProperty("activity.records.to.generate", 100000, Integer::parseInt);
    ConcurrentMap<? extends Class<?>, List<Object>> entitiesByType = IntStream.range(0, activityRecords)
        .parallel()
        .mapToObj(i -> {
          var node = StorageNode.random(maxNodes);
          var item = StockItem.random(maxItems);
          var record = StorageNodeActivity.random(node.getId(), item.getId());
          return Stream.of(node, item, record);
        }).flatMap(Function.identity())
        .distinct()
        .collect(Collectors.groupingByConcurrent(Object::getClass));



    Class.forName("org.postgresql.Driver");
    int parallelism = Runtime.getRuntime().availableProcessors() * 8;
    var connectionsByThread = new ConcurrentHashMap<String, Connection>();

    var executor = Executors.newFixedThreadPool(parallelism);
    final int batchSize = 1000;
    List<Future<Void>> futures = executor
        .invokeAll(tasks(connectionsByThread, maxNodes, maxItems, activityRecords, batchSize));
    executor.shutdown();
    boolean finished = executor.awaitTermination(5, TimeUnit.MINUTES);
    checkState(finished, "The timeout elapsed before all tasks executed");
  }

  private static Collection<Callable<Void>> tasks(
      ConcurrentHashMap<String, Connection> connectionsByThread,
      int maxNodes,
      int maxItems,
      int activityRecords,
      int batchSize) {
    var tasks = Collections.nCopies(activityRecords / batchSize, task(connectionsByThread, maxNodes,
        maxItems, batchSize));
    if (activityRecords % batchSize > 0) {
      tasks = new ArrayList<>(tasks);
      tasks.add(task(connectionsByThread, maxNodes, maxItems, activityRecords % batchSize));
    }
    return tasks;
  }

  private static Callable<Void> task(ConcurrentHashMap<String, Connection> connectionsByThread,
      int maxNodes, int maxItems, int inserts) {
    return () -> {
      var conn = getConnection(connectionsByThread);
      try (var insertNode = conn.prepareStatement(INSERT_NODE);
          var insertItem = conn.prepareStatement(INSERT_ITEM);
          var insertActivity = conn.prepareStatement(INSERT_ACTIVITY)) {
        for (int i = 0; i < inserts; i++) {
          var node = StorageNode.random(maxNodes);
          insertNode.setInt(1, node.getId());
          insertNode.setString(2, node.getName());
          insertNode.addBatch();

          var item = StockItem.random(maxItems);
          insertItem.setInt(1, item.getId());
          insertItem.setString(2, item.getDescription());
          insertItem.addBatch();

          var activity = StorageNodeActivity.random(node.getId(), item.getId());
          insertActivity.setInt(1, activity.getStorageNodeId());
          insertActivity.setInt(2, activity.getStockItemId());
          insertActivity.setTimestamp(
              3, new Timestamp(activity.getMoment().toInstant().toEpochMilli()));
          insertActivity.setInt(4, activity.getQuantity());
          insertActivity.addBatch();
        }
        insertNode.executeBatch();
        insertItem.executeBatch();
        insertActivity.executeBatch();
      }

      try {
        conn.commit();
        System.out.println("BATCH COMPLETED");
      } catch (SQLException e) {
        conn.rollback();
        e.printStackTrace();
        System.exit(1);
        throw new IllegalStateException(e);
      }

      return null;
    };
  }

  private static Connection getConnection(ConcurrentHashMap<String, Connection> connectionsByThread) {
    return connectionsByThread.computeIfAbsent(Thread.currentThread().getName(), t -> {
      try {
        var connection = DriverManager.getConnection("jdbc:postgresql:connect_test", "postgres", "postgres");
        connection.setAutoCommit(false);
        return connection;
      } catch (SQLException e) {
        e.printStackTrace();
        System.exit(1);
        throw new IllegalStateException(e);
      }
    });
  }

  private static <T> T getProperty(String property, Function<String, T> converter) {
    return getProperty(property, null, converter);
  }

  private static <T> T getProperty(String property, Integer defaultValue, Function<String, T> converter) {
    return converter.apply(System.getProperty(property, Objects.toString(defaultValue)));
  }


}
