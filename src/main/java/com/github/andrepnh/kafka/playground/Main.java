package com.github.andrepnh.kafka.playground;

import com.github.andrepnh.kafka.playground.db.gen.Generator;
import com.github.andrepnh.kafka.playground.db.gen.StockItem;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.github.andrepnh.kafka.playground.db.gen.StockState;
import com.google.common.collect.Lists;
import com.google.common.collect.Tables;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Main {
  private static final String INSERT_WAREHOUSE =
      "INSERT INTO Warehouse(id, name, latitude, longitude, storageCapacity) VALUES (?, ?, ?, ?, ?) "
          + "ON CONFLICT DO NOTHING";

  private static final String INSERT_ITEM =
      "INSERT INTO StockItem(id, description) VALUES (?, ?) ON CONFLICT DO NOTHING";

  private static final String INSERT_STOCK =
      "INSERT INTO StockState (warehouseId, stockItemId, supply, demand, reserved) "
    + "VALUES (?, ?, ?, ?, ?) "
    + "ON CONFLICT (warehouseId, stockItemId) DO UPDATE SET supply = ?, demand = ?, reserved = ?";

  public static void main(String[] args) {
    var maxWarehouses = getProperty("max.warehouses", 300, Integer::parseInt);
    var maxItems = getProperty("max.items", 1000000, Integer::parseInt);
    var stocks = getProperty("stock.to.generate", 10000, Integer::parseInt);
    Supplier<Connection> dbConnectionSupplier = createDbConnectionSupplier();

    Stream
        .generate(() -> {
          var warehouse = Warehouse.random(maxWarehouses);
          var item = StockItem.random(maxItems);
          return Tables.immutableCell(warehouse, item, StockState.random(warehouse.getId(), item.getId()));
        })
        .parallel()
        .limit(stocks)
        .peek(unused -> {
          try {
            TimeUnit.MILLISECONDS.sleep(12);
          } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
          }
        })
        .forEach(triple -> {
          Warehouse warehouse = triple.getRowKey();
          StockItem item = triple.getColumnKey();
          StockState stock = triple.getValue();
          insert(warehouse, dbConnectionSupplier);
          insert(item, dbConnectionSupplier);
          insert(stock, dbConnectionSupplier);
        });
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

  private static void insert(StockState stock, Supplier<Connection> connectionSupplier) {
    try (var preparedStatement = connectionSupplier.get().prepareStatement(INSERT_STOCK)) {
      preparedStatement.setInt(1, stock.getWarehouseId());
      preparedStatement.setInt(2, stock.getStockItemId());
      preparedStatement.setInt(3, stock.getSupply());
      preparedStatement.setInt(4, stock.getDemand());
      preparedStatement.setInt(5, stock.getReserved());
      preparedStatement.setInt(6, stock.getSupply());
      preparedStatement.setInt(7, stock.getDemand());
      preparedStatement.setInt(8, stock.getReserved());
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
