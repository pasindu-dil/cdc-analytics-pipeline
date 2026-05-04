### Setup environment in docker

    docker compose up -d

### Connector configurations

This command registers a Debezium MySQL connector with Kafka Connect, which will capture changes from the specified MySQL database and publish them to Kafka topics. The configuration includes details about the MySQL connection, the tables to monitor, and how to handle the data transformation.

    curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @kafka-connect/debezium-connector.json

This command deletes the previously registered Debezium MySQL connector named "mysql-shopdb-connector" from Kafka Connect, stopping any further data capture from the MySQL database and removing the connector's configuration.

    curl -X DELETE http://localhost:8083/connectors/mysql-bsmsc-connector

This command retrieves the status of the Debezium MySQL connector named "mysql-shopdb-connector" from Kafka Connect, providing information about whether the connector is running, any errors, and the status of its tasks.

    curl -s http://localhost:8083/connectors/mysql-bsmsc-connector/status




### Execute clickhouse schema in clickhouse client

### Copy to container

    docker cp clickhouse_schema.sql clickhouse:/tmp/ 

### Execute

    docker exec -it clickhouse clickhouse-client --user root --password root --multiquery --queries-file /tmp/clickhouse_schema.sql
    