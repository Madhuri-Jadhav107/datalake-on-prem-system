#!/bin/bash

# --- Optimized Configuration for 1.6 Billion Records ---
source_host="mysql-source"
source_port="3306"
source_user="user"
source_pass="password"
source_db="inventory"

echo "Registering EXTREME PERFORMANCE Debezium Connector for MySQL..."

curl -X POST -H "Content-Type: application/json" --data '{
  "name": "mysql-inventory-connector-optimized",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "4",
    "database.hostname": "'"$source_host"'",
    "database.port": "'"$source_port"'",
    "database.user": "'"$source_user"'",
    "database.password": "'"$source_pass"'",
    "database.server.id": "223346",
    "database.server.name": "mysql_source",
    "database.include.list": "'"$source_db"'",
    "topic.prefix": "mysql_source_optimized",
    "table.include.list": "inventory.customers",
    
    # --- Snapshot Optimization ---
    "snapshot.mode": "initial",
    "snapshot.locking.mode": "none",
    "snapshot.max.threads": "4",
    
    # --- Throughput Tuning ---
    "max.batch.size": "20480",
    "max.queue.size": "81920",
    "poll.interval.ms": "100",
    
    # --- Kafka Serialization ---
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory.optimized"
  }
}' http://localhost:18083/connectors/

echo -e "\nOptimized Connector registered. Check status: http://localhost:18083/connectors/mysql-inventory-connector-optimized/status"
