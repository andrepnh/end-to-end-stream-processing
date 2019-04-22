package com.github.andrepnh.kafka.playground.stream;

import static org.junit.Assert.assertEquals;

import com.github.andrepnh.kafka.playground.db.StockQuantity;
import com.github.andrepnh.kafka.playground.db.Warehouse;
import com.github.andrepnh.kafka.playground.stream.GlobalStockUpdatesSource.QuantityWrapper;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

public class GlobalStockUpdatesSourceTest extends BaseStreamTest {

  @Test
  public void shouldComputeGlobalForMultipleStockItems() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    final var item1 = stockQuantity(warehouse, 1, 10);
    final var item2 = stockQuantity(warehouse, 2, 33);
    pipe(item1, item2);
    pipe(warehouse);
    List<ProducerRecord<Integer, QuantityWrapper>> records =
        readAll("global-stock", JsonSerde.of(Integer.class), JsonSerde.of(QuantityWrapper.class));
    assertEquals(2, records.size());
    assertEquals(item1.getStockItemId(), (int) records.get(0).key());
    assertEquals(item1.getQuantity(), records.get(0).value().getQuantity());
    assertEquals(item2.getStockItemId(), (int) records.get(1).key());
    assertEquals(item2.getQuantity(), records.get(1).value().getQuantity());
  }

  @Test
  public void shouldUpdateGlobalWithUpdatedQuantitiesFromWarehouse() {
    final int item = 3, originalQuantity = 10, updatedQuantity = 200;
    final Warehouse warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    pipe(stockQuantity(warehouse, item, originalQuantity),
        stockQuantity(warehouse, item, updatedQuantity));
    pipe(warehouse);
    ProducerRecord<Integer, QuantityWrapper> record =
        readLast("global-stock", JsonSerde.of(Integer.class), JsonSerde.of(QuantityWrapper.class));
    assertEquals(item, (int) record.key());
    assertEquals(updatedQuantity, record.value().getQuantity());
  }

  @Test
  public void shouldSumQuantitiesFromSameItemAcrossDifferentWarehouses() {
    final int item = 12, warehouse1Quantity = 33, warehouse2Quantity = 29;
    final Warehouse warehouse1 = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now()),
        warehouse2 = new Warehouse(2, "two", 500, 0, 0, ZonedDateTime.now());
    pipe(stockQuantity(warehouse1, item, warehouse1Quantity),
        stockQuantity(warehouse2, item, warehouse2Quantity));
    pipe(warehouse1, warehouse2);
    ProducerRecord<Integer, QuantityWrapper> record =
        readLast("global-stock", JsonSerde.of(Integer.class), JsonSerde.of(QuantityWrapper.class));
    assertEquals(item, (int) record.key());
    assertEquals(warehouse1Quantity + warehouse2Quantity, record.value().getQuantity());
  }

  @Test
  public void shouldNotConsiderLateUpdates() {
    final Warehouse warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    final StockQuantity firstUpdate = stockQuantity(warehouse, 1, 10),
        lateUpdate = new StockQuantity(warehouse.getId(), firstUpdate.getStockItemId(), firstUpdate.getQuantity() + 10, firstUpdate.getLastUpdate().minusSeconds(1));
    pipe(firstUpdate, lateUpdate);
    pipe(warehouse);
    ProducerRecord<Integer, QuantityWrapper> record =
        readLast("global-stock", JsonSerde.of(Integer.class), JsonSerde.of(QuantityWrapper.class));
    assertEquals(firstUpdate.getStockItemId(), (int) record.key());
    assertEquals(firstUpdate.getQuantity(), record.value().getQuantity());
  }

}
