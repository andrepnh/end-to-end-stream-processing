CREATE STREAM warehouse_raw (after STRUCT<id INTEGER, name STRING, latitude DOUBLE, longitude DOUBLE, storagecapacity INTEGER, lastupdate BIGINT>) WITH (KAFKA_TOPIC = 'connect_test.public.warehouse', VALUE_FORMAT = 'json');

CREATE STREAM stock_quantity_raw (after STRUCT<warehouseid INTEGER, stockitemid INTEGER, quantity INTEGER, lastupdate BIGINT>) WITH (KAFKA_TOPIC = 'connect_test.public.stockquantity', VALUE_FORMAT = 'JSON');

CREATE STREAM warehouse_stream WITH (TIMESTAMP = 'last_update', KAFKA_TOPIC = 'warehouse_stream') AS SELECT CAST(after->id AS STRING) AS id, after->name AS name, after->latitude AS latitude, after->longitude AS longitude, after->storagecapacity AS storage_capacity, after->lastupdate / 100000 AS last_update FROM warehouse_raw PARTITION BY id;

CREATE TABLE warehouse (id STRING, name STRING, latitude DOUBLE, longitude DOUBLE, storage_capacity INTEGER, last_update BIGINT) WITH (KAFKA_TOPIC = 'warehouse_stream', VALUE_FORMAT = 'json', KEY = 'id', TIMESTAMP = 'last_update');

CREATE STREAM stock_quantity WITH (TIMESTAMP = 'last_update') AS SELECT CAST(after->warehouseid AS STRING) AS warehouse_id, CAST(after->stockitemid AS STRING) AS stock_item_id, after->quantity AS quantity, after->lastupdate / 100000 AS last_update FROM stock_quantity_raw;

CREATE TABLE global_stock_table WITH (TIMESTAMP = 'last_update', KAFKA_TOPIC = 'global_stock') AS SELECT stock_item_id AS id, SUM(quantity) AS quantity, MAX(last_update) AS last_update FROM stock_quantity GROUP BY stock_item_id;

-- Most demanded stock items
CREATE TABLE high_demand_stock WITH (TIMESTAMP = 'last_update', KAFKA_TOPIC = 'high_demand_stock') AS SELECT stock_item_id, SUM(quantity) AS demand, MAX(last_update) AS last_update FROM stock_quantity WHERE quantity < 0 GROUP BY stock_item_id;

-- Warehouse allocation
CREATE TABLE warehouse_stock WITH (TIMESTAMP = 'last_update') AS SELECT warehouse_id AS id, SUM(quantity) AS quantity, MAX(last_update) AS last_update FROM stock_quantity GROUP BY warehouse_id;
CREATE TABLE warehouse_allocation WITH (TIMESTAMP = 'last_update', KAFKA_TOPIC = 'warehouse_allocation') AS SELECT w.id AS id, w.name, w.latitude, w.longitude, CAST(s.quantity AS DOUBLE) / w.storage_capacity AS allocation, s.last_update AS last_update FROM warehouse_stock s JOIN warehouse w ON s.id = w.id;

-- Least available stock items based on history
-- Since we can't call MIN and MAX when selecting from tables
CREATE STREAM global_stock_stream (id STRING, quantity INTEGER, last_update BIGINT) WITH (KAFKA_TOPIC = 'global_stock', VALUE_FORMAT = 'JSON', KEY = 'id', TIMESTAMP = 'last_update');
-- The table below won't have a key field set - you can check that with DESCRIBE EXTENDED - and due to a bug in KSQL we'd get a 
-- NullPointerException if we try to use it in the JOIN as we'll do below (see https://github.com/confluentinc/ksql/issues/1686)
-- To fix it we do a simple CREATE TABLE backed by the same topic as the tmp table and specify KEY as usual
CREATE TABLE stock_global_minmax_tmp WITH (TIMESTAMP = 'last_update', KAFKA_TOPIC = 'stock_global_minmax') AS SELECT id, MIN(quantity) AS min_quantity, MAX(quantity) AS max_quantity, MAX(last_update) AS last_update FROM global_stock_stream GROUP BY id;
CREATE TABLE stock_global_minmax (id STRING, min_quantity INTEGER, max_quantity INTEGER, last_update BIGINT) WITH (KAFKA_TOPIC = 'stock_global_minmax', KEY = 'id', VALUE_FORMAT = 'json', TIMESTAMP = 'last_update');
CREATE TABLE stock_global_percentage WITH (TIMESTAMP = 'last_update', KAFKA_TOPIC = 'stock_global_percentage') AS SELECT m.id AS ID, (CAST(s.quantity AS DOUBLE) - m.min_quantity) / (m.max_quantity - m.min_quantity) AS percentage, s.last_update AS last_update FROM stock_global_minmax m JOIN global_stock_table s ON s.id = m.id;