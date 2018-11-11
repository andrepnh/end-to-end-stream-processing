package com.github.andrepnh.kafka.playground;

import com.github.andrepnh.kafka.playground.stream.GlobalStockUpdatesSource;
import com.github.andrepnh.kafka.playground.stream.HighDemandStockSource;
import com.github.andrepnh.kafka.playground.stream.StockGlobalPercentageSource;
import com.github.andrepnh.kafka.playground.stream.StockQuantityUpdatesSource;
import com.github.andrepnh.kafka.playground.stream.StreamProperties;
import com.github.andrepnh.kafka.playground.stream.WarehouseAllocationSource;
import com.github.andrepnh.kafka.playground.stream.WarehouseStockSource;
import java.util.UUID;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class Main {
  public static void main(String[] args) {
    var processor = new Main();
    var topology = processor.buildTopology();
    var properties = StreamProperties.newDefaultStreamProperties(UUID.randomUUID().toString());
    System.out.println(topology.describe());
    var streams = new KafkaStreams(topology, properties);
    streams.cleanUp();
    streams.setUncaughtExceptionHandler((thread, throwable) -> {
      throwable.printStackTrace();
      streams.close();
      System.exit(1);
    });
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public Topology buildTopology() {
    var builder = new StreamsBuilder();

    var stockUpdatesSource = StockQuantityUpdatesSource.build(builder);
    var warehouseStockSource = WarehouseStockSource.from(stockUpdatesSource);
    var warehouseAllocationSource = WarehouseAllocationSource.build(builder, warehouseStockSource);
    var globalStockUpdatesSource = GlobalStockUpdatesSource.from(warehouseStockSource);
    var highDemandStockSource = HighDemandStockSource.from(stockUpdatesSource);
    var stockGlobalPercentageSource = StockGlobalPercentageSource.from(globalStockUpdatesSource);

    warehouseAllocationSource.sinkTo("warehouse-allocation");
    globalStockUpdatesSource.sinkTo("global-stock");
    highDemandStockSource.sinkTo("high-demand-stock");
    stockGlobalPercentageSource.sinkTo("stock-global-percentage");

    return builder.build();
  }
}
