package com.github.andrepnh.kafka.playground.stream;

import static org.junit.Assert.assertEquals;

import com.github.andrepnh.kafka.playground.db.StockQuantity;
import com.github.andrepnh.kafka.playground.db.Warehouse;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

public class HighDemandStockSourceTest extends BaseStreamTest {

  @Test
  public void shouldComputeStockWithHighDemandForMultipleStockItems() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    final var item1 = stockQuantity(warehouse, 1, -10);
    final var item2 = stockQuantity(warehouse, 2, -33);
    pipe(item1, item2);
    pipe(warehouse);
    List<ProducerRecord<Integer, Quantity>> records =
        readAll("high-demand-stock", JsonSerde.of(Integer.class), JsonSerde.of(Quantity.class));
    assertEquals(2, records.size());
    assertEquals(item1.getStockItemId(), (int) records.get(0).key());
    assertEquals(item1.getQuantity(), records.get(0).value().getQuantity());
    assertEquals(item2.getStockItemId(), (int) records.get(1).key());
    assertEquals(item2.getQuantity(), records.get(1).value().getQuantity());
  }

  @Test
  public void shouldSumDemandFromSameItemAcrossDifferentWarehouses() {
    final int item = 12, warehouse1Quantity = -33, warehouse2Quantity = -27;
    final Warehouse warehouse1 = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now()),
        warehouse2 = new Warehouse(2, "two", 500, 0, 0, ZonedDateTime.now());
    pipe(warehouse1, warehouse2);
    pipe(stockQuantity(warehouse1, item, warehouse1Quantity),
        stockQuantity(warehouse2, item, warehouse2Quantity));
    ProducerRecord<Integer, Quantity> record =
        readLast("high-demand-stock", JsonSerde.of(Integer.class), JsonSerde.of(Quantity.class));
    assertEquals(item, (int) record.key());
    assertEquals(warehouse1Quantity + warehouse2Quantity, record.value().getQuantity());
  }

  @Test
  public void shouldNotConsiderLateUpdates() {
    final Warehouse warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    final StockQuantity firstUpdate = stockQuantity(warehouse, 1, -5),
        lateUpdate = new StockQuantity(warehouse.getId(),
            firstUpdate.getStockItemId(),
            firstUpdate.getQuantity() - 10,
            firstUpdate.getLastUpdate().minusSeconds(1));
    pipe(firstUpdate, lateUpdate);
    pipe(warehouse);
    ProducerRecord<Integer, Quantity> record =
        readLast("high-demand-stock", JsonSerde.of(Integer.class), JsonSerde.of(Quantity.class));
    assertEquals(firstUpdate.getStockItemId(), (int) record.key());
    assertEquals(firstUpdate.getQuantity(), record.value().getQuantity());
  }

  @Test
  public void shouldNotConsiderSupply() {
    final Warehouse warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    final StockQuantity demand = stockQuantity(warehouse, 1, -5),
        supply = stockQuantity(warehouse, 1, Math.abs(demand.getQuantity()));
    pipe(demand, supply);
    pipe(warehouse);
    ProducerRecord<Integer, Quantity> record =
        readLast("high-demand-stock", JsonSerde.of(Integer.class), JsonSerde.of(Quantity.class));
    assertEquals(demand.getStockItemId(), (int) record.key());
    assertEquals(demand.getQuantity(), record.value().getQuantity());
  }
}
