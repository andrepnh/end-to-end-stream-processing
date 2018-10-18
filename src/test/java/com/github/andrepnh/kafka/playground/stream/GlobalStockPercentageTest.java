package com.github.andrepnh.kafka.playground.stream;

import static org.junit.Assert.assertEquals;

import com.github.andrepnh.kafka.playground.db.gen.StockQuantity;
import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.github.andrepnh.kafka.playground.stream.StreamProcessor.IdWrapper;
import com.github.andrepnh.kafka.playground.stream.StreamProcessor.PercentageWrapper;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

public class GlobalStockPercentageTest extends BaseStreamTest {

  @Test
  public void shouldComputeGlobalPercentagesForMultipleStockItems() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    final StockQuantity item1Min = stockQuantity(warehouse, 1, 0),
        item1Max = stockQuantity(warehouse, 1, 50),
        item1Current = stockQuantity(warehouse, 1, 25);
    final StockQuantity item2Min = stockQuantity(warehouse, 2, 337),
        item2Max = stockQuantity(warehouse, 2, 5023),
        item2Current = stockQuantity(warehouse, 2, 4184);
    pipe(item1Min, item1Max, item1Current, item2Min, item2Max, item2Current);
    pipe(warehouse);
    List<ProducerRecord<IdWrapper, PercentageWrapper>> records =
        readAll("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    var item1LastRecord = records
        .stream()
        .filter(record -> record.key().getId() == 1)
        .reduce((first, second) -> second)
        .get();
    assertEquals(computePercentage(item1Min, item1Current, item1Max),
        item1LastRecord.value().getPercentage(),
        0.000001);
    var item2LastRecord = records
        .stream()
        .filter(record -> record.key().getId() == 2)
        .reduce((first, second) -> second)
        .get();
    assertEquals(computePercentage(item2Min, item2Current, item2Max),
        item2LastRecord.value().getPercentage(),
        0.000001);
  }

  @Test
  public void shouldUpdateGlobalPercentageWhenANewHighestQuantityIsSent() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    StockQuantity min = stockQuantity(warehouse, 1, 0),
        max = stockQuantity(warehouse, 1, 100),
        current = stockQuantity(warehouse, 1, 33);
    pipe(min, max, current);
    pipe(warehouse);
    var record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(min.getStockItemId(), record.key().getId());
    assertEquals(computePercentage(min, current, max), record.value().getPercentage(), 0.000001);
    current = max = stockQuantity(warehouse, 1, max.getQuantity() * 2);
    pipe(current);
    record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(computePercentage(min, current, max), record.value().getPercentage(), 0.000001);
    current = stockQuantity(warehouse, 1, 111);
    pipe(current);
    record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(computePercentage(min, current, max), record.value().getPercentage(), 0.000001);
  }

  @Test
  public void shouldUpdateGlobalPercentageWhenANewLowestQuantityIsSent() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    StockQuantity min = stockQuantity(warehouse, 1, 0),
        max = stockQuantity(warehouse, 1, 100),
        current = stockQuantity(warehouse, 1, 33);
    pipe(min, max, current);
    pipe(warehouse);
    var record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(min.getStockItemId(), record.key().getId());
    assertEquals(computePercentage(min, current, max), record.value().getPercentage(), 0.000001);
    current = min = stockQuantity(warehouse, 1, min.getQuantity() -31);
    pipe(current);
    record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(computePercentage(min, current, max), record.value().getPercentage(), 0.000001);
    current = stockQuantity(warehouse, 1, 44);
    pipe(current);
    record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(computePercentage(min, current, max), record.value().getPercentage(), 0.000001);
  }

  @Test
  public void shouldNotConsiderLateUpdates() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    StockQuantity min = stockQuantity(warehouse, 1, 0),
        max = stockQuantity(warehouse, 1, 100),
        current = stockQuantity(warehouse, 1, 33);
    pipe(min, max, current);
    pipe(warehouse);
    var record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(min.getStockItemId(), record.key().getId());
    assertEquals(computePercentage(min, current, max), record.value().getPercentage(), 0.000001);
    var lateUpdate = new StockQuantity(warehouse.getId(), 1, max.getQuantity() * 2, max.getLastUpdate().minusSeconds(1));
    pipe(lateUpdate);
    record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(computePercentage(min, current, max), record.value().getPercentage(), 0.000001);
  }

  @Test
  public void shouldComputeGlobalStockPercentage() {
    final var warehouse = new Warehouse(1, "one", 1000, 5, 5, ZonedDateTime.now());
    StockQuantity min = stockQuantity(warehouse, 1, 0),
        max = stockQuantity(warehouse, 1, 100),
        last = stockQuantity(warehouse, 1, 50);

    pipe(min);
    var record = readSingle("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(min.getStockItemId(), record.key().getId());
    assertEquals(Double.NaN, record.value().getPercentage(), 0.000000000001);
    pipe(max);
    record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(min.getStockItemId(), record.key().getId());
    assertEquals(1.0, record.value().getPercentage(), 0.000000000001);
    pipe(last);
    record = readLast("global-stock-percentage", JsonSerde.of(IdWrapper.class), JsonSerde.of(PercentageWrapper.class));
    assertEquals(min.getStockItemId(), record.key().getId());
    assertEquals(0.5, record.value().getPercentage(), 0.000000000001);
  }

  private double computePercentage(StockQuantity min, StockQuantity current, StockQuantity max) {
    return (double) (current.getQuantity() - min.getQuantity())
        / (max.getQuantity() - min.getQuantity());
  }


}
