#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE connect_test;
    \connect connect_test;

    CREATE TABLE StockItem (
        id              int PRIMARY KEY,
        description     varchar(40) NOT NULL
    );

    CREATE TABLE Warehouse (
        id                  int PRIMARY KEY,
        name                varchar(40) NOT NULL,
        latitude            real NOT NULL,
        longitude           real NOT NULL,
        storageCapacity     int NOT NULL
    );

    CREATE TABLE StockQuantity (
        warehouseId         int NOT NULL REFERENCES Warehouse,
        stockItemId         int NOT NULL REFERENCES StockItem,
        quantity            int NOT NULL,
        lastUpdate          timestamp NOT NULL,
        PRIMARY KEY(warehouseId, stockItemId)
    );
EOSQL