#!/bin/bash
set -e

echo "####################################################"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER kconnect WITH PASSWORD 'kconnect';
    CREATE DATABASE connect_test;
    GRANT ALL PRIVILEGES ON DATABASE connect_test TO kconnect;
EOSQL