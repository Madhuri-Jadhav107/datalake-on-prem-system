#!/bin/bash

# Configuration for Debezium Postgres Connector
# This script sends the configuration to the Debezium REST API

source_host="source-db"
source_port="5432"
source_user="user"
source_pass="password"
source_db="inventory"

echo "Registering Debezium Connector for PostgreSQL..."

curl -X POST -H "Content-Type: application/json" --data '{
  "name": "inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "'"$source_host"'",
    "database.port": "'"$source_port"'",
    "database.user": "'"$source_user"'",
    "database.password": "'"$source_pass"'",
    "database.dbname": "'"$source_db"'",
    "database.server.name": "source_db",
    "topic.prefix": "source_db",
    "table.include.list": "public.customers",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot"
  }
}' http://localhost:18083/connectors/

echo -e "\nConnector registration request sent. Check status at http://localhost:18083/connectors/inventory-connector/status"
