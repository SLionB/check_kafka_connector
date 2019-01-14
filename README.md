# check_kafka_connector
Check failed Kafka tasks and take the appropriate action

There is an open issue in Kafka Connect where Kafka JDBC Connector (Sink or Source) stops if connection with the Database breaks from the database part.
https://github.com/confluentinc/kafka-connect-jdbc/issues/515

This program checks a JDBC connector status by name using Connect API (at the configured server IP address and port) and restarts failed tasks.
