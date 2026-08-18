# Debezium CDC Prototype

A hands-on learning project to understand **Change Data Capture (CDC)** using:

- Java
- Spring Boot 3.x
- MySQL 8.0.37
- Apache Kafka
- Kafka Connect
- Debezium
- Docker

The prototype starts with a simple **Order Service** and demonstrates how changes made by the Spring Boot application in MySQL are captured by Debezium and published to Kafka.

## Architecture

```text
Spring Boot Order Service
          |
          | INSERT / UPDATE / DELETE
          v
     MySQL 8.0.37
       ord_db.orders
          |
          | MySQL Binary Log
          v
   Kafka Connect + Debezium
          |
          v
        Kafka
          |
          v
dbserver1.ord_db.orders
```

**Environment:** MySQL runs directly on Windows. Kafka and Kafka Connect/Debezium run inside Docker. Debezium reaches Windows MySQL through `host.docker.internal:3306`.

## Project Structure

```text
debezium-prototype/
|
+-- order-service/
|   +-- Spring Boot application
|
+-- infrastructure/
    +-- docker-compose.yml
    +-- connector.json
```

## 1. Prerequisites

Install:

- Java
- Maven
- MySQL 8.0.37
- Docker Desktop for Windows
- Git

Verify Docker:

```powershell
docker --version
docker compose version
```

## 2. MySQL Configuration

Debezium reads the MySQL binary log to capture changes.

The prototype uses:

```text
log_bin            ON
binlog_format      ROW
binlog_row_image   FULL
server_id          1
```

Verify:

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
SHOW VARIABLES LIKE 'server_id';
SHOW BINARY LOGS;
```

## 3. Create the Debezium MySQL User

Create a dedicated user:

```sql
CREATE USER 'debezium_cdc'@'%' IDENTIFIED BY 'YOUR_PASSWORD';
```

Grant the required permissions:

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, LOCK TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT
ON *.*
TO 'debezium_cdc'@'%';
```

Verify:

```sql
SHOW GRANTS FOR 'debezium_cdc'@'%';
```

The username used by this prototype is:

```text
debezium_cdc
```

**Do not commit the real database password to GitHub.**

## 4. Order Database

The prototype uses:

```text
Database: ord_db
Table:    orders
```

The current table includes fields such as:

```text
id
created_at
customer_id
price
product
quantity
status
```

## 5. Docker Infrastructure

The Docker environment contains:

```text
Kafka
Kafka Connect
Debezium MySQL Connector
```

Ports:

```text
Kafka         localhost:9092
Kafka Connect localhost:8083
```

Inside Docker, Kafka Connect communicates with Kafka using:

```text
kafka:29092
```

## 6. Start Kafka and Debezium

Go to the infrastructure directory:

Start:

```powershell
docker compose up -d
```

Check:

```powershell
docker ps
```

You should see containers similar to:

```text
kafka
debezium-connect
```

## 7. Verify Kafka Connect

Check Kafka Connect:

```powershell
Invoke-RestMethod http://localhost:8083
```

Check installed connector plugins:

```powershell
Invoke-RestMethod http://localhost:8083/connector-plugins
```

The important plugin is:

```text
io.debezium.connector.mysql.MySqlConnector
```

## 8. Debezium Connector

The connector captures:

```text
ord_db.orders
```

Core configuration:

```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.hostname": "host.docker.internal",
  "database.port": "3306",
  "database.user": "debezium_cdc",
  "database.password": "YOUR_PASSWORD",
  "database.server.id": "184054",
  "topic.prefix": "dbserver1",
  "database.include.list": "ord_db",
  "table.include.list": "ord_db.orders",
  "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
  "schema.history.internal.kafka.topic": "schemahistory.ord_db",
  "snapshot.mode": "initial"
}
```

### `database.server.id`

MySQL uses:

```text
server_id = 1
```

Debezium uses:

```text
database.server.id = 184054
```

They must be different because Debezium connects using MySQL replication.

## 9. Register the Connector

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  --data "@connector.json"
```

Check status:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/mysql-order-connector/status
```

Expected:

```text
connector = RUNNING
task       = RUNNING
```

## 10. Kafka Topics

List topics:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:29092 `
  --list
```

The prototype currently produces topics including:

```text
__consumer_offsets
connect_configs
connect_offsets
connect_status
dbserver1
dbserver1.ord_db.orders
schemahistory.ord_db
```

The main application CDC topic is:

```text
dbserver1.ord_db.orders
```

It represents:

```text
Database: ord_db
Table:    orders
```

## 11. Consume Debezium Events

Start a consumer:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:29092 `
  --topic dbserver1.ord_db.orders `
  --from-beginning
```

Then create or modify an order using the Spring Boot Order Service.

The flow is:

```text
Spring Boot
    |
    v
MySQL INSERT / UPDATE / DELETE
    |
    v
MySQL Binary Log
    |
    v
Debezium
    |
    v
Kafka
    |
    v
dbserver1.ord_db.orders
```

## 12. Understanding a Debezium Event

A Debezium message contains a `schema` and a `payload`.

Example:

```json
{
  "schema": {
    "..."
  },
  "payload": {
    "before": null,
    "after": {
      "id": 2,
      "customer_id": 11,
      "price": "...",
      "product": "One plus 17 pro ulti-max",
      "quantity": 1,
      "status": "CREATED"
    },
    "source": {
      "connector": "mysql",
      "name": "dbserver1",
      "db": "ord_db",
      "table": "orders",
      "server_id": 1
    },
    "op": "c",
    "ts_ms": 1786954782272
  }
}
```

### `before`

The row before the change.

For an INSERT:

```json
"before": null,
```

### `after`

The row after the change.

### `source`

Metadata about the source database event, including database, table, MySQL server ID and binary-log position.

### `op`

Operation code:

```text
c = create / INSERT
u = update / UPDATE
d = delete / DELETE
r = read / snapshot
```

### `snapshot`

Debezium can indicate whether an event came from a snapshot or the live change stream:

```json
"snapshot": "true"
```

or:

```json
"snapshot": "false"
```

## 13. Decimal Values

The prototype encountered:

```json
"price": "GfCg"
```

The schema showed a Kafka Connect Decimal backed by bytes:

```json
{
  "type": "bytes",
  "name": "org.apache.kafka.connect.data.Decimal",
  "parameters": {
    "scale": "2"
  },
  "field": "price"
}
```

For a learning-friendly representation, the connector can use:

```json
"decimal.handling.mode": "string"
```

Then a value can appear in a form such as:

```json
"price": "150000.00"
```

This is one of the topics to continue exploring in the prototype.

## 14. Stop and Start Docker

Stop the containers without removing them:

```powershell
docker compose stop
```

Start them again:

```powershell
docker compose start
```

Stop and remove containers:

```powershell
docker compose down
```

Avoid during the learning phase:

```powershell
docker compose down -v
```

because removing volumes can remove persisted Kafka/Kafka Connect state.

## 15. Learning Progress

Completed:

- [x] Create Spring Boot Order Service
- [x] Configure MySQL 8.0.37
- [x] Enable MySQL binary logging
- [x] Configure `ROW` binlog format
- [x] Configure `FULL` row image
- [x] Configure MySQL `server_id`
- [x] Create `debezium_cdc` user
- [x] Install Docker Desktop
- [x] Start Kafka
- [x] Start Kafka Connect
- [x] Verify Debezium MySQL connector
- [x] Register MySQL Debezium connector
- [x] Verify connector is `RUNNING`
- [x] Verify Kafka topics
- [x] Consume a real Debezium CDC event
- [x] Understand `before` / `after`
- [x] Understand `op`
- [x] Understand snapshot metadata
- [x] Identify MySQL `DECIMAL` representation

## 16. Next Steps

```text
1. Configure decimal.handling.mode
2. Demonstrate initial snapshot
3. Test INSERT CDC
4. Test UPDATE CDC
5. Test DELETE CDC
6. Understand Debezium events in depth
7. Build a Spring Boot Kafka consumer
8. Convert Debezium events into application events
9. Build a downstream service
10. Explore real-world CDC patterns
```

The goal is not just to make Debezium work, but to understand **why each component exists and how data moves from a database transaction to a Kafka event**.

## Technologies

```text
Java
Spring Boot 3.x
MySQL 8.0.37
Apache Kafka
Kafka Connect
Debezium
Docker
Maven
```

## Learning Goal

This repository is a hands-on prototype for understanding:

- Change Data Capture (CDC)
- MySQL binary logs
- Debezium
- Kafka Connect
- Kafka topics
- Debezium event envelopes
- Snapshot vs streaming changes
- INSERT / UPDATE / DELETE CDC events
- Database-to-event-driven architecture
