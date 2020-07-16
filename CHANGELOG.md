# MongoDB Kafka Connector

## Changelog

## 1.3.0
  - [KAFKA-129](https://jira.mongodb.org/browse/KAFKA-129) Added support for Bson bytes in the Sink connector.

## 1.2.0
  - [KAFKA-92](https://jira.mongodb.org/browse/KAFKA-92) Allow the Sink connector to use multiple tasks.
  - [KAFKA-116](https://jira.mongodb.org/browse/KAFKA-116) Ensure the MongoCopyDataManager doesn't fail when the source is a non-existent database.
  - [KAFKA-111](https://jira.mongodb.org/browse/KAFKA-111) Fix Source connector copying existing resumability
  - [KAFKA-110](https://jira.mongodb.org/browse/KAFKA-110) Added `document.id.strategy.overwrite.existing` configuration.
    Note: This defaults to false, which is a change of behaviour.
  - [KAFKA-118](https://jira.mongodb.org/browse/KAFKA-118) Made UuidStrategy configurable so can output BsonBinary Uuid values
  - [KAFKA-101](https://jira.mongodb.org/browse/KAFKA-101) Added `UuidProvidedInKeyStrategy` & `UuidProvidedInValueStrategy`
  - [KAFKA-114](https://jira.mongodb.org/browse/KAFKA-114) Added `UpdateOneBusinessKeyTimestampStrategy` write model strategy`
  - [KAFKA-112](https://jira.mongodb.org/browse/KAFKA-112) Added `BlockList` and `AllowList` field projector type configurations and
    `BlockListKeyProjector`, `BlockListValueProjector`, `AllowListKeyProjector`and `AllowListValueProjector` Post processors.
    Deprecated: `BlacklistKeyProjector`, `BlacklistValueProjector`, `WhitelistKeyProjector` and `WhitelistValueProjector`.
  - [KAFKA-75](https://jira.mongodb.org/browse/KAFKA-75) Added specific configuration for the id strategies: `ProvidedInKeyStrategy` and `ProvidedInValueStrategy`.
    Added `document.id.strategy.partial.value.projection.type`, `document.id.strategy.partial.value.projection.list`,
    `document.id.strategy.partial.key.projection.type` and `document.id.strategy.partial.key.projection.list`.
  - [KAFKA-91](https://jira.mongodb.org/browse/KAFKA-91) Improved the error messaging for the missing resume tokens in the source connector.

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
