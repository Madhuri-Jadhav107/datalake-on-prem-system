#!/bin/bash

# Configuration for Debezium MySQL Connector
source_host="mysql-source"
source_port="3306"
source_user="user"
source_pass="password"
source_db="inventory"

echo "Registering Debezium Connector for MySQL..."

curl -X POST -H "Content-Type: application/json" --data '{
  "name": "mysql-inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "'"$source_host"'",
    "database.port": "'"$source_port"'",
    "database.user": "'"$source_user"'",
    "database.password": "'"$source_pass"'",
    "database.server.id": "223344",
    "database.server.name": "mysql_source",
    "database.include.list": "'"$source_db"'",
    "topic.prefix": "mysql_source",
    "table.include.list": "inventory.customers",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory"
  }
}' http://localhost:18083/connectors/

echo -e "\nMySQL Connector registration request sent. Check status at http://localhost:18083/connectors/mysql-inventory-connector/status"
