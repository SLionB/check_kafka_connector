# check_kafka_connector
Check failed Kafka tasks and take the appropriate action

There is an open issue in Kafka Connect where Kafka JDBC Connector (Sink or Source) stops if connection with the Database breaks from the database part.
https://github.com/confluentinc/kafka-connect-jdbc/issues/515

This program checks a JDBC connector status by name using Connect API (at the configured server IP address and port) and restarts failed tasks.


## Run check_kafka_connector
```
go run check_kafka_connector.go -s={kafka_connect_server}:{kafka_connect_port} -c={kafka_connector_name}
```
## Defaults for check_kafka_connector
```
{kafka_connect_server}:{kafka_connect_port} = localhost:8083 
{kafka_connector_name} = connector
```