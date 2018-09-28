package com.github.andrepnh.kafka.playground.stream;

import static org.junit.Assert.assertEquals;

import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class GlobalStockPercentageTest extends BaseStreamTest {

  @Test
  public void shouldComputeGlobalPercentagesForMultipleStockItems() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5);
    final StockQuantity item1Min = stockQuantity(warehouse, 1, 0),
        item1Max = stockQuantity(warehouse, 1, 50),
        item1Current = stockQuantity(warehouse, 1, 25);
    final StockQuantity item2Min = stockQuantity(warehouse, 2, 337),
        item2Max = stockQuantity(warehouse, 2, 5023),
        item2Current = stockQuantity(warehouse, 2, 4184);
    pipe(item1Min, item1Max, item1Current, item2Min, item2Max, item2Current);
    pipe(warehouse);
    List<ProducerRecord<Integer, Double>> records =
        readAll("global-stock-percentages", Serdes.Integer(), Serdes.Double());
    assertEquals(2, records.size());
    assertEquals(item1Min.getStockItemId(), (int) records.get(0).key());
    assertEquals(computePercentage(item1Min, item1Current, item1Max), records.get(0).value(), 0.000001);
    assertEquals(item2Min.getStockItemId(), (int) records.get(1).key());
    assertEquals(computePercentage(item2Min, item2Current, item2Max), records.get(1).value(), 0.000001);
  }

  @Test
  public void shouldUpdateGlobalPercentageWhenANewHighestQuantityIsSent() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5);
    StockQuantity min = stockQuantity(warehouse, 1, 0),
        max = stockQuantity(warehouse, 1, 100),
        current = stockQuantity(warehouse, 1, 33);
    pipe(min, max, current);
    pipe(warehouse);
    var record = readSingle("global-stock-percentages", Serdes.Integer(), Serdes.Double());
    assertEquals(min.getStockItemId(), (int) record.key());
    assertEquals(computePercentage(min, current, max), record.value(), 0.000001);
    current = max = stockQuantity(warehouse, 1, max.getQuantity() * 2);
    pipe(current);
    assertEquals(computePercentage(min, current, max), record.value(), 0.000001);
    current = stockQuantity(warehouse, 1, 111);
    pipe(current);
    assertEquals(computePercentage(min, current, max), record.value(), 0.000001);
  }

  @Test
  public void shouldUpdateGlobalPercentageWhenANewLowestQuantityIsSent() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5);
    StockQuantity min = stockQuantity(warehouse, 1, 0),
        max = stockQuantity(warehouse, 1, 100),
        current = stockQuantity(warehouse, 1, 33);
    pipe(min, max, current);
    pipe(warehouse);
    var record = readSingle("global-stock-percentages", Serdes.Integer(), Serdes.Double());
    assertEquals(min.getStockItemId(), (int) record.key());
    assertEquals(computePercentage(min, current, max), record.value(), 0.000001);
    current = min = stockQuantity(warehouse, 1, min.getQuantity() -31);
    pipe(current);
    assertEquals(computePercentage(min, current, max), record.value(), 0.000001);
    current = stockQuantity(warehouse, 1, 44);
    pipe(current);
    assertEquals(computePercentage(min, current, max), record.value(), 0.000001);
  }

  @Test
  public void shouldNotConsiderLateUpdates() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5);
    StockQuantity min = stockQuantity(warehouse, 1, 0),
        max = stockQuantity(warehouse, 1, 100),
        current = stockQuantity(warehouse, 1, 33);
    pipe(min, max, current);
    pipe(warehouse);
    var record = readSingle("global-stock-percentages", Serdes.Integer(), Serdes.Double());
    assertEquals(min.getStockItemId(), (int) record.key());
    assertEquals(computePercentage(min, current, max), record.value(), 0.000001);
    var lateUpdate = new StockQuantity(warehouse.getId(), 1, max.getQuantity() * 2, max.getLastUpdate().minusSeconds(1));
    pipe(lateUpdate);
    assertEquals(computePercentage(min, current, max), record.value(), 0.000001);
  }

  private double computePercentage(StockQuantity min, StockQuantity current, StockQuantity max) {
    return (double) (current.getQuantity() - min.getQuantity())
        / (max.getQuantity() - current.getQuantity());
  }


}
