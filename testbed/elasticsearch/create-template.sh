curl -X PUT http://$DOCKER_HOST_IP:9200/_template/kafka-connect-template \
-H "Accept: application/json" -H "Content-Type: application/json" \
-d @- << EOF
{
  "index_patterns": ["warehouse-capacity", "global-stock", "global-stock-percentage"],
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
        "@timestamp": {
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
