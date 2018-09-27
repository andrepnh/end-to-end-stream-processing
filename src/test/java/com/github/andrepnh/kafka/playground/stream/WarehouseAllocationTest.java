package com.github.andrepnh.kafka.playground.stream;

import static org.junit.Assert.assertEquals;

import com.github.andrepnh.kafka.playground.db.gen.Warehouse;
import com.google.common.collect.MoreCollectors;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

public class WarehouseAllocationTest extends BaseStreamTest {

  @Test
  public void warehouseAllocationThresholdShouldUpdateWhenStockItemQuantityIsReplaced() {
    final int capacity = 100,
        lowQuantity = (int) (AllocationThreshold.LOW.getThreshold() * capacity),
        normalQuantity = lowQuantity + 1,
        highQuantity = (int) (AllocationThreshold.NORMAL.getThreshold() * capacity) + 1;
    final Warehouse warehouse1 = new Warehouse(1, "one", capacity, 50, 50);
    pipe(stockQuantity(warehouse1, 1, lowQuantity));
    pipe(warehouse1);
    ProducerRecord<WarehouseKey, Allocation> record =
        read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals(AllocationThreshold.LOW, record.value().getThreshold());

    pipe(stockQuantity(warehouse1, 1, normalQuantity));
    pipe(warehouse1);
    record = read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals(AllocationThreshold.NORMAL, record.value().getThreshold());

    pipe(stockQuantity(warehouse1, 1, highQuantity));
    pipe(warehouse1);
    record = read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals(AllocationThreshold.HIGH, record.value().getThreshold());
  }

  @Test
  public void warehouseAllocationShouldUpdateWhenAStockItemQuantityIsReplaced() {
    final int capacity = 100, quantity1 = 10, quantity2 = 50;
    final Warehouse warehouse1 = new Warehouse(1, "one", capacity, 50, 50);
    pipe(stockQuantity(warehouse1, 1, quantity1));
    pipe(warehouse1);
    ProducerRecord<WarehouseKey, Allocation> record =
        read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals((double) quantity1 / capacity, record.value().getAllocation(), 0.000001);

    pipe(stockQuantity(warehouse1, 1, quantity2));
    pipe(warehouse1);
    record = read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    assertEquals((double) quantity2 / capacity, record.value().getAllocation(), 0.000001);
  }

  @Test
  public void warehouseAllocationShouldSumQuantityFromAllDifferentStockItems() {
    final int capacity = 200, item1 = 10, item2 = 90;
    final Warehouse warehouse = new Warehouse(1, "one", capacity, 50, 50);
    pipe(stockQuantity(warehouse, 1, item1), stockQuantity(warehouse, 2, item2));
    pipe(warehouse);
    ProducerRecord<WarehouseKey, Allocation> record =
        read("warehouse-capacity", JsonSerde.of(WarehouseKey.class), JsonSerde.of(Allocation.class));
    var allocation = (double) (item1 + item2) / capacity;
    assertEquals(allocation, record.value().getAllocation(), 0.000001);
  }

  @Test
  public void warehouseAllocationShouldBeIndependentFromOtherWarehouses() {
    final int warehouse1Capacity = 100,
        warehouse2Capacity = 200,
        item1DemandOnWarehouse1 = 10,
        item1DemandOnWarehouse2 = 100;
    final Warehouse warehouse1 = new Warehouse(1, "one", warehouse1Capacity, 50, 50),
        warehouse2 = new Warehouse(2, "two", warehouse2Capacity, 25 ,-25);

    pipe(stockQuantity(warehouse1, 1, item1DemandOnWarehouse1),
        stockQuantity(warehouse2, 1, item1DemandOnWarehouse2));
    pipe(warehouse1, warehouse2);
    Map<Integer, ProducerRecord<WarehouseKey, Allocation>> recordByWarehouse =
        readStream(
            "warehouse-capacity",
            JsonSerde.of(WarehouseKey.class),
            JsonSerde.of(Allocation.class))
        .collect(Collectors.groupingBy(
            record -> record.key().getId(),
            MoreCollectors.onlyElement()));
    assertEquals((double) item1DemandOnWarehouse1 / warehouse1Capacity,
        recordByWarehouse.get(warehouse1.getId()).value().getAllocation(),
        0.000001);
    assertEquals((double) item1DemandOnWarehouse2 / warehouse2Capacity,
        recordByWarehouse.get(warehouse2.getId()).value().getAllocation(),
        0.000001);
  }
}
