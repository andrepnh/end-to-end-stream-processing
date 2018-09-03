#!/bin/bash

curl -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d @connector-config.json http://localhost:8083/connectors