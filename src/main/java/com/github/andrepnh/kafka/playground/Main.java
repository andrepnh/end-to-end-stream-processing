package com.github.andrepnh.kafka.playground;

import static com.google.common.base.Preconditions.checkState;

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
  private static final int IO_PARALLELISM = Runtime.getRuntime().availableProcessors() * 8;

  private static final ExecutorService IO_EXECUTOR = Executors.newFixedThreadPool(IO_PARALLELISM);

  private static final String INSERT_NODE = "INSERT INTO StorageNode(id, name) VALUES (?, ?)";

  private static final String INSERT_ITEM = "INSERT INTO StockItem(id, description) VALUES (?, ?)";

  private static final String INSERT_ACTIVITY = "INSERT INTO StorageNodeActivity"
      + "(storageNodeId, stockItemId, moment, quantity) VALUES (?, ?, ?, ?)";

  public static final Set<Object> INSERTED = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public static void main(String[] args) throws InterruptedException {
    var maxNodes = getProperty("max.storage.nodes", 3000, Integer::parseInt);
    var maxItems = getProperty("max.stock.items", 10000000, Integer::parseInt);
    var activityRecords = getProperty("activity.records.to.generate", 100000, Integer::parseInt);

    var nodes = Collections.newSetFromMap(new ConcurrentHashMap<StorageNode, Boolean>(maxNodes));
    var items = Collections.newSetFromMap(new ConcurrentHashMap<StockItem, Boolean>(Math.min(maxItems, activityRecords)));
    var activities = Collections.newSetFromMap(new ConcurrentHashMap<StorageNodeActivity, Boolean>(activityRecords));
    populateInParallel(nodes, items, activities, maxNodes, maxItems, activityRecords);

    List<Future<Void>> nodeFutures = IO_EXECUTOR
        .invokeAll(insertNodesWorkers(IO_PARALLELISM / 2, nodes, 1000));
    List<Future<Void>> itemFutures = IO_EXECUTOR
        .invokeAll(insertItemsWorkers(IO_PARALLELISM, items, 1000));
    checkSuccessfulInserts(nodeFutures, itemFutures);

    List<Future<Void>> activityFutures = IO_EXECUTOR
        .invokeAll(insertActivitiesWorkers(IO_PARALLELISM, activities, 1000));
    IO_EXECUTOR.shutdown();
    checkSuccessfulInserts(activityFutures);
  }

  private static void checkSuccessfulInserts(List<Future<Void>> futures) {
    checkSuccessfulInserts(futures, new ArrayList<>());
  }

  private static void checkSuccessfulInserts(List<Future<Void>> futures1, List<Future<Void>> futures2) {
    Stream.concat(futures1.stream(), futures2.stream())
        .forEach(future -> {
          try {
            future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
          }
        });
  }

  private static void populateInParallel(Set<StorageNode> nodes, Set<StockItem> items,
      Set<StorageNodeActivity> activities, int maxNodes,
      int maxItems,
      int activityRecords) {
    Stream
        .generate(() -> {
          var node = StorageNode.random(maxNodes);
          var item = StockItem.random(maxItems);
          var record = StorageNodeActivity.random(node.getId(), item.getId());
          return Stream.of(node, item, record);
        })
        .parallel()
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

  private static Collection<Callable<Void>> insertNodesWorkers(int workers, Set<StorageNode> entities, int batchSize) {
    BiConsumer<PreparedStatement, StorageNode> setParameters = (insert, node) -> {
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

  private static Collection<Callable<Void>> insertItemsWorkers(int workers, Set<StockItem> entities, int batchSize) {
    BiConsumer<PreparedStatement, StockItem> setParameters = (insert, item) -> {
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

  private static Collection<Callable<Void>> insertActivitiesWorkers(int workers, Set<StorageNodeActivity> entities, int batchSize) {
    BiConsumer<PreparedStatement, StorageNodeActivity> setParameters = (insert, activity) -> {
      try {
        insert.setInt(1, activity.getStorageNodeId());
        insert.setInt(2, activity.getStockItemId());
        insert.setTimestamp(
            3, new Timestamp(activity.getMoment().toInstant().toEpochMilli()));
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

  private static <T> T getProperty(String property, Integer defaultValue, Function<String, T> converter) {
    return converter.apply(System.getProperty(property, Objects.toString(defaultValue)));
  }

  private static class Worker<T> implements Callable<Void> {
    private final ConcurrentLinkedQueue<T> entities;
    private final String insert;
    private final int batchSize;
    private final BiConsumer<PreparedStatement, T> setParameters;
    private final boolean debug;

    public Worker(Set<T> entities, String insert, int batchSize,
        BiConsumer<PreparedStatement, T> setParameters) {
      this(entities, insert, batchSize, setParameters, false);
    }

    public Worker(Set<T> entities, String insert, int batchSize,
        BiConsumer<PreparedStatement, T> setParameters, boolean debug) {
      this.entities = new ConcurrentLinkedQueue<>(entities);
      this.insert = insert;
      this.batchSize = batchSize;
      this.setParameters = setParameters;
      this.debug = debug;
    }

    @Override
    public Void call() throws Exception {
      try (var conn = DriverManager.getConnection("jdbc:postgresql://192.168.99.100/connect_test", "postgres", "postgres")) {
        var preparedStatement = conn.prepareStatement(insert);
        conn.setAutoCommit(false);
        var entitiesRemaining = false;
        for (int row = 0; !entities.isEmpty(); row++) {
          var entity = entities.poll();
          if (entity == null) {
            break;
          }
          checkState(!debug || INSERTED.add(entity));
          setParameters.accept(preparedStatement, entity);
          preparedStatement.addBatch();
          entitiesRemaining = true;
          if (row != 0 && row % batchSize == 0) {
            preparedStatement = commitBatch(conn, preparedStatement);
            System.out.format("%d inserted\n", batchSize);
            entitiesRemaining = false;
          }
        }

        if (entitiesRemaining) {
          commitBatch(conn, preparedStatement).close();
          System.out.println("Inserted a few remaining entities");
        }

        return null;
      }
    }

    private PreparedStatement commitBatch(Connection conn, PreparedStatement preparedStatement) {
      try {
        preparedStatement.executeBatch();
        conn.commit();
        preparedStatement.clearParameters();
        preparedStatement.close();
        return conn.prepareStatement(insert);
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
