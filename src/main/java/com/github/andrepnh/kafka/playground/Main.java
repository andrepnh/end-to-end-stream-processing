package com.github.andrepnh.kafka.playground;

import com.github.andrepnh.kafka.playground.db.gen.StockItem;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import com.github.andrepnh.kafka.playground.stream.StreamProcessor;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.Tables;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Main {
  private static final String INSERT_WAREHOUSE =
      "INSERT INTO Warehouse(id, name, latitude, longitude, storageCapacity, lastUpdate) "
          + "VALUES (?, ?, ?, ?, ?, ?) "
          + "ON CONFLICT DO NOTHING";

  private static final String INSERT_ITEM =
      "INSERT INTO StockItem(id, description) VALUES (?, ?) ON CONFLICT DO NOTHING";

  private static final String INSERT_STOCK =
      "INSERT INTO StockQuantity (warehouseId, stockItemId, quantity, lastUpdate) "
    + "VALUES (?, ?, ?, ?) "
    + "ON CONFLICT (warehouseId, stockItemId) DO "
    + "UPDATE SET quantity = ?, lastUpdate = ?";

  public static void main(String[] args) throws IOException {
    if (!(args.length > 0 && "generate".equals(args[0]))) {
      StreamProcessor.main(args);
      return;
    }
    var maxWarehouses = getProperty("max.warehouses", 300, Integer::parseInt);
    var maxItems = getProperty("max.items", 500, Integer::parseInt);
    var stocks = getProperty("stock.to.generate", 100000, Integer::parseInt);
    Supplier<Connection> dbConnectionSupplier = createDbConnectionSupplier();
    Supplier<Cell<Warehouse, StockItem, StockQuantity>> randomTripleSupplier = () -> {
      var warehouse = Warehouse.random(maxWarehouses);
      var item = StockItem.random(maxItems);
      return Tables.immutableCell(warehouse, item,
          StockQuantity.random(warehouse.getId(), item.getId()));
    };

    final var millisecondsSleep = new AtomicInteger(100);
    final var insertions = new AtomicInteger(0);
    var pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    pool.submit(() -> {
      Stream
          .iterate(randomTripleSupplier, supplier -> supplier)
          .parallel()
          .limit(stocks)
          .forEach(supplier -> {
            Cell<Warehouse, StockItem, StockQuantity> triple = supplier.get();
            Warehouse warehouse = triple.getRowKey();
            StockItem item = triple.getColumnKey();
            StockQuantity stock = triple.getValue();
            insert(warehouse, dbConnectionSupplier);
            insert(item, dbConnectionSupplier);
            insert(stock, dbConnectionSupplier);
            if (insertions.incrementAndGet() % 100 == 0) {
              System.out.println(LocalTime.now() + " - 100 records inserted");
            }
            try {
              TimeUnit.MILLISECONDS.sleep(millisecondsSleep.get());
            } catch (InterruptedException e) {
              throw new IllegalStateException(e);
            }
          });
    });
    try (var reader = new BufferedReader(new InputStreamReader(System.in))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        int sleep = Integer.parseInt(line);
        if (sleep == -1) {
          System.exit(0);
        }
        millisecondsSleep.set(sleep);
      }
    }
  }

  private static Supplier<Connection> createDbConnectionSupplier() {
    var connectionString =
        getProperty(
            "db.connections.string",
            String.format("jdbc:postgresql://%s/connect_test", System.getenv("DOCKER_HOST_IP")),
            Function.identity());
    var user = getProperty("db.user", "postgres", Function.identity());
    var password = getProperty("db.password", "postgres", Function.identity());
    var connectionsByThread = new ConcurrentHashMap<Thread, Connection>();
    return () -> connectionsByThread
        .computeIfAbsent(Thread.currentThread(), (unused) -> {
          try {
            return DriverManager.getConnection(connectionString, user, password);
          } catch (SQLException ex) {
            throw new IllegalStateException(ex);
          }
        });
  }

  private static void insert(Warehouse warehouse, Supplier<Connection> connectionSupplier) {
    try (var preparedStatement = connectionSupplier.get().prepareStatement(INSERT_WAREHOUSE)) {
      preparedStatement.setInt(1, warehouse.getId());
      preparedStatement.setString(2, warehouse.getName());
      preparedStatement.setFloat(3, warehouse.getLatitude());
      preparedStatement.setFloat(4, warehouse.getLongitude());
      preparedStatement.setInt(5, warehouse.getStorageCapacity());
      preparedStatement.setTimestamp(6, new Timestamp(warehouse.getLastUpdate().toInstant().toEpochMilli()));
      preparedStatement.execute();
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private static void insert(StockItem item, Supplier<Connection> connectionSupplier) {
    try (var preparedStatement = connectionSupplier.get().prepareStatement(INSERT_ITEM)) {
      preparedStatement.setInt(1, item.getId());
      preparedStatement.setString(2, item.getDescription());
      preparedStatement.execute();
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private static void insert(StockQuantity stock, Supplier<Connection> connectionSupplier) {
    try (var preparedStatement = connectionSupplier.get().prepareStatement(INSERT_STOCK)) {
      preparedStatement.setInt(1, stock.getWarehouseId());
      preparedStatement.setInt(2, stock.getStockItemId());
      preparedStatement.setInt(3, stock.getQuantity());
      preparedStatement.setTimestamp(4, new Timestamp(stock.getLastUpdate().toInstant().toEpochMilli()));
      preparedStatement.setInt(5, stock.getQuantity());
      preparedStatement.setTimestamp(6, new Timestamp(stock.getLastUpdate().toInstant().toEpochMilli()));
      preparedStatement.execute();
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private static <T> T getProperty(String property, Function<String, T> converter) {
    return getProperty(property, null, converter);
  }

  private static <T> T getProperty(String property, T defaultValue, Function<String, T> converter) {
    return converter.apply(System.getProperty(property, Objects.toString(defaultValue)));
  }

}
