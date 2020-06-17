# MongoDB Kafka Connector

## Changelog

## 1.2.0
  - [KAFKA-92](https://jira.mongodb.org/browse/KAFKA-92) Allow the Sink connector to use multiple tasks.
  - [KAFKA-116](https://jira.mongodb.org/browse/KAFKA-116) Ensure the MongoCopyDataManager doesn't fail when the source is a non-existent database.

## 1.1.0
  - [KAFKA-45](https://jira.mongodb.org/browse/KAFKA-45) Allow the Sink connector to ignore unused source record key or value fields.
  - [KAFKA-82](https://jira.mongodb.org/browse/KAFKA-82) Added support for "topics.regex" in the Sink connector.
  - [KAFKA-84](https://jira.mongodb.org/browse/KAFKA-84) Validate the connection via `MongoSìnkConnector.validate` or `MongoSourceConnector.validate`
  - [KAFKA-95](https://jira.mongodb.org/browse/KAFKA-95) Fixed Issue with "Unrecognized field: startAfter" in the Source connector for older MongoDB versions

## 1.0.1
  - [KAFKA-86](https://jira.mongodb.org/browse/KAFKA-86) Fixed Source connector resumability error.
  - [KAFKA-85](https://jira.mongodb.org/browse/KAFKA-85) Fixed Source connector IllegalStateException: Queue full when copying data.
  - [KAFKA-83](https://jira.mongodb.org/browse/KAFKA-83) Fixed Source connector cursor resumability when filtering operationTypes.

## 1.0.0

The initial GA release.
