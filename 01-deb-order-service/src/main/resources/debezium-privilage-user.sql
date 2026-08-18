
#creating debezium user

CREATE USER 'debezium_cdc'@'%' IDENTIFIED BY 'root';

GRANT SELECT,
      RELOAD,
      SHOW DATABASES,
      REPLICATION SLAVE,
      REPLICATION CLIENT,
      LOCK TABLES
ON *.*
TO 'debezium_cdc'@'%';

FLUSH PRIVILEGES;

SHOW GRANTS FOR 'debezium_cdc'@'%';

SHOW BINARY LOGS;