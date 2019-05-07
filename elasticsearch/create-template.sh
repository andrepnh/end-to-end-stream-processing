curl -f -X PUT http://$DOCKER_HOST_IP:9200/_template/inventory-template \
-H "Accept: application/json" -H "Content-Type: application/json" \
-d "@index-templates.json"