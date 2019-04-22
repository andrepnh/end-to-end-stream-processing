curl -X PUT http://$DOCKER_HOST_IP:9200/_template/inventory-template \
-H "Accept: application/json" -H "Content-Type: application/json" \
-d @- << EOF
{
  "index_patterns": [
    "warehouse-allocation", "global-stock", "stock-global-percentage", "high-demand-stock", 
    "warehouse_allocation", "global_stock", "stock_global_percentage", "high_demand_stock"
  ],
  "order": 0,
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "5s"
  },
  "mappings": {
    "kafka-connect": {
      "_source": {
        "enabled": true
      },
      "properties": {
        "last_update": {
          "type": "date"
        },
        "lastUpdate": {
          "type": "date"
        },
        "location": {
          "type": "geo_point"
        },
        "threshold": {
            "type": "keyword"
        }
      }
    }
  }
}
EOF
