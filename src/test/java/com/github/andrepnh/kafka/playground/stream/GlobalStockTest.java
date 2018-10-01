package com.github.andrepnh.kafka.playground.stream;

import static org.junit.Assert.assertEquals;

import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class GlobalStockTest extends BaseStreamTest {

  @Test
  public void shouldComputeGlobalForMultipleStockItems() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5);
    final var item1 = stockQuantity(warehouse, 1, 10);
    final var item2 = stockQuantity(warehouse, 2, 33);
    pipe(item1, item2);
    pipe(warehouse);
    List<ProducerRecord<Integer, Integer>> records =
        readAll("global-stock", Serdes.Integer(), Serdes.Integer());
    assertEquals(2, records.size());
    assertEquals(item1.getStockItemId(), (int) records.get(0).key());
    assertEquals(item1.getQuantity(), (int) records.get(0).value());
    assertEquals(item2.getStockItemId(), (int) records.get(1).key());
    assertEquals(item2.getQuantity(), (int) records.get(1).value());
  }

  @Test
  public void shouldUpdateGlobalWithUpdatedQuantitiesFromWarehouse() {
    final int item = 3, originalQuantity = 10, updatedQuantity = 200;
    final Warehouse warehouse = new Warehouse(1, "one", 1000, 5, 5);
    pipe(stockQuantity(warehouse, item, originalQuantity),
        stockQuantity(warehouse, item, updatedQuantity));
    pipe(warehouse);
    ProducerRecord<Integer, Integer> record =
        readLast("global-stock", Serdes.Integer(), Serdes.Integer());
    assertEquals(item, (int) record.key());
    assertEquals(updatedQuantity, (int) record.value());
  }

  @Test
  public void shouldSumQuantitiesFromSameItemAcrossDifferentWarehouses() {
    final int item = 12, warehouse1Quantity = 33, warehouse2Quantity = 29;
    final Warehouse warehouse1 = new Warehouse(1, "one", 1000, 5, 5),
        warehouse2 = new Warehouse(2, "two", 500, 0, 0);
    pipe(stockQuantity(warehouse1, item, warehouse1Quantity),
        stockQuantity(warehouse2, item, warehouse2Quantity));
    pipe(warehouse1, warehouse2);
    ProducerRecord<Integer, Integer> record =
        readLast("global-stock", Serdes.Integer(), Serdes.Integer());
    assertEquals(item, (int) record.key());
    assertEquals(warehouse1Quantity + warehouse2Quantity, (int) record.value());
  }

  @Test
  public void shouldNotConsiderLateUpdates() {
    final Warehouse warehouse = new Warehouse(1, "one", 1000, 5, 5);
    final StockQuantity firstUpdate = stockQuantity(warehouse, 1, 10),
        lateUpdate = new StockQuantity(warehouse.getId(), firstUpdate.getStockItemId(), firstUpdate.getQuantity() + 10, firstUpdate.getLastUpdate().minusSeconds(1));
    pipe(firstUpdate, lateUpdate);
    pipe(warehouse);
    ProducerRecord<Integer, Integer> record =
        readLast("global-stock", Serdes.Integer(), Serdes.Integer());
    assertEquals(firstUpdate.getStockItemId(), (int) record.key());
    assertEquals(firstUpdate.getQuantity(), (int) record.value());
  }

}
