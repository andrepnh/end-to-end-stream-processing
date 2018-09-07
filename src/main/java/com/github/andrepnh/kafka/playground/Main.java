package com.github.andrepnh.kafka.playground;

import com.github.andrepnh.kafka.playground.db.gen.StockItem;
import com.github.andrepnh.kafka.playground.db.gen.StorageNode;
import com.github.andrepnh.kafka.playground.db.gen.StorageNodeActivity;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
  private static final int IO_PARALLELISM;

  private static final ExecutorService IO_EXECUTOR;

  private static final String INSERT_NODE = "INSERT INTO StorageNode(id, name) VALUES (?, ?)";

  private static final String INSERT_ITEM = "INSERT INTO StockItem(id, description) VALUES (?, ?)";

  private static final String INSERT_ACTIVITY =
      "INSERT INTO StorageNodeActivity"
          + "(storageNodeId, stockItemId, moment, quantity) VALUES (?, ?, ?, ?)";

  private static final String DB_CONNECTION_STRING;

  private static final String DB_USER;

  private static final String DB_PASSWORD;

  static {
    IO_PARALLELISM = getProperty(
        "io.parallelism",
        Runtime.getRuntime().availableProcessors() * 2,
        Integer::parseInt);
    IO_EXECUTOR = Executors.newFixedThreadPool(IO_PARALLELISM);
    DB_CONNECTION_STRING =
        getProperty(
            "db.connections.string",
            String.format("jdbc:postgresql://%s/connect_test", System.getenv("DOCKER_HOST_IP")),
            Function.identity());
    DB_USER = getProperty("db.connections.string", "postgres", Function.identity());
    DB_PASSWORD = getProperty("db.connections.string", "postgres", Function.identity());
  }

  public static void main(String[] args) throws InterruptedException {
    var maxNodes = getProperty("max.storage.nodes", 300, Integer::parseInt);
    var maxItems = getProperty("max.stock.items", 1000000, Integer::parseInt);
    var activityRecords = getProperty("activity.records.to.generate", 10000, Integer::parseInt);

    var nodes = Collections.newSetFromMap(new ConcurrentHashMap<StorageNode, Boolean>(maxNodes));
    var items = Collections.newSetFromMap(
        new ConcurrentHashMap<StockItem, Boolean>(Math.min(maxItems, activityRecords)));
    var activities = Collections.newSetFromMap(
        new ConcurrentHashMap<StorageNodeActivity, Boolean>(activityRecords));
    populateInParallel(nodes, items, activities, maxNodes, maxItems, activityRecords);

    List<Future<Void>> nodeFutures = IO_EXECUTOR.invokeAll(
        insertNodesWorkers(IO_PARALLELISM, new ConcurrentLinkedQueue<>(nodes), 1000));
    List<Future<Void>> itemFutures = IO_EXECUTOR.invokeAll(
        insertItemsWorkers(IO_PARALLELISM, new ConcurrentLinkedQueue<>(items), 1000));
    checkSuccessfulInserts(nodeFutures, itemFutures);

    List<Future<Void>> activityFutures = IO_EXECUTOR.invokeAll(
        insertActivitiesWorkers(IO_PARALLELISM, new ConcurrentLinkedQueue<>(activities), 1000));
    IO_EXECUTOR.shutdown();
    checkSuccessfulInserts(activityFutures);
  }

  private static void checkSuccessfulInserts(List<Future<Void>> futures) {
    checkSuccessfulInserts(futures, new ArrayList<>());
  }

  private static void checkSuccessfulInserts(
      List<Future<Void>> futures1, List<Future<Void>> futures2) {
    Stream.concat(futures1.stream(), futures2.stream())
        .forEach(future -> {
          try {
            future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
          }
        });
  }

  private static void populateInParallel(
      Set<StorageNode> nodes,
      Set<StockItem> items,
      Set<StorageNodeActivity> activities,
      int maxNodes,
      int maxItems,
      int activityRecords) {
    Stream.generate(() -> {
          var node = StorageNode.random(maxNodes);
          var item = StockItem.random(maxItems);
          var record = StorageNodeActivity.random(node.getId(), item.getId());
          return Stream.of(node, item, record);
        }).parallel()
        .limit(activityRecords)
        .flatMap(Function.identity())
        .forEach(entity -> {
          if (entity instanceof StorageNode) {
            nodes.add((StorageNode) entity);
          } else if (entity instanceof StockItem) {
            items.add((StockItem) entity);
          } else {
            activities.add((StorageNodeActivity) entity);
          }
        });
  }

  private static Collection<Callable<Void>> insertNodesWorkers(
      int workers, ConcurrentLinkedQueue<StorageNode> entities, int batchSize) {
    BiConsumer<PreparedStatement, StorageNode> setParameters =
        (insert, node) -> {
          try {
            insert.setInt(1, node.getId());
            insert.setString(2, node.getName());
          } catch (SQLException e) {
            throw new IllegalStateException(e);
          }
        };
    return Stream.generate(() -> new Worker<>(entities, INSERT_NODE, batchSize, setParameters))
        .limit(workers)
        .collect(Collectors.toList());
  }

  private static Collection<Callable<Void>> insertItemsWorkers(
      int workers, ConcurrentLinkedQueue<StockItem> entities, int batchSize) {
    BiConsumer<PreparedStatement, StockItem> setParameters =
        (insert, item) -> {
          try {
            insert.setInt(1, item.getId());
            insert.setString(2, item.getDescription());
          } catch (SQLException e) {
            throw new IllegalStateException(e);
          }
        };
    return Stream.generate(() -> new Worker<>(entities, INSERT_ITEM, batchSize, setParameters))
        .limit(workers)
        .collect(Collectors.toList());
  }

  private static Collection<Callable<Void>> insertActivitiesWorkers(
      int workers, ConcurrentLinkedQueue<StorageNodeActivity> entities, int batchSize) {
    BiConsumer<PreparedStatement, StorageNodeActivity> setParameters =
        (insert, activity) -> {
          try {
            insert.setInt(1, activity.getStorageNodeId());
            insert.setInt(2, activity.getStockItemId());
            insert.setTimestamp(3, new Timestamp(activity.getMoment().toInstant().toEpochMilli()));
            insert.setInt(4, activity.getQuantity());
          } catch (SQLException e) {
            throw new IllegalStateException(e);
          }
        };
    return Stream.generate(() -> new Worker<>(entities, INSERT_ACTIVITY, batchSize, setParameters))
        .limit(workers)
        .collect(Collectors.toList());
  }

  private static <T> T getProperty(String property, Function<String, T> converter) {
    return getProperty(property, null, converter);
  }

  private static <T> T getProperty(String property, T defaultValue, Function<String, T> converter) {
    return converter.apply(System.getProperty(property, Objects.toString(defaultValue)));
  }

  private static class Worker<T> implements Callable<Void> {
    private final ConcurrentLinkedQueue<T> entities;
    private final String insert;
    private final int batchSize;
    private final BiConsumer<PreparedStatement, T> setParameters;
    private final boolean debug;

    public Worker(
        ConcurrentLinkedQueue<T> entities,
        String insert,
        int batchSize,
        BiConsumer<PreparedStatement, T> setParameters) {
      this(entities, insert, batchSize, setParameters, false);
    }

    public Worker(
        ConcurrentLinkedQueue<T> entities,
        String insert,
        int batchSize,
        BiConsumer<PreparedStatement, T> setParameters,
        boolean debug) {
      this.entities = entities;
      this.insert = insert;
      this.batchSize = batchSize;
      this.setParameters = setParameters;
      this.debug = debug;
    }

    @Override
    public Void call() throws Exception {
      try (var conn = DriverManager.getConnection(DB_CONNECTION_STRING, DB_USER, DB_PASSWORD);
           var preparedStatement = conn.prepareStatement(insert)) {
        conn.setAutoCommit(false);
        int entitiesRemaining = 0;
        for (int row = 0; !entities.isEmpty(); row++) {
          var entity = entities.poll();
          if (entity == null) {
            break;
          }
          setParameters.accept(preparedStatement, entity);
          preparedStatement.addBatch();
          entitiesRemaining++;
          if (row != 0 && row % batchSize == 0) {
            commitBatch(conn, preparedStatement);
            System.out.format("%d inserted\n", batchSize);
            entitiesRemaining = 0;
          }
        }

        if (entitiesRemaining > 0) {
          commitBatch(conn, preparedStatement);
          System.out.format("%d inserted\n", entitiesRemaining);
        }

        return null;
      }
    }

    private void commitBatch(Connection conn, PreparedStatement preparedStatement) {
      try {
        preparedStatement.executeBatch();
        conn.commit();
        preparedStatement.clearParameters();
      } catch (SQLException e) {
        try {
          conn.rollback();
        } catch (SQLException e1) {
          throw new IllegalStateException(e);
        }
        throw new IllegalStateException(e);
      }
    }
  }
}
