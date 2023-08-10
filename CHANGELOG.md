# MongoDB Kafka Connector

## Changelog

## 1.11.0

### Improvements
  - [KAFKA-274](https://jira.mongodb.org/browse/KAFKA-274) Made Debezium (DBZ) ddl events a noop
  - [KAFKA-360](https://jira.mongodb.org/browse/KAFKA-360) Added configuration `change.stream.document.key.as.key` and defaults to true.
    Previously, the resume token was used as the key, however, that limits the usefulness of tombstones and topic compaction. Set to false to revert.

### Bug Fixes
  - [KAFKA-378](https://jira.mongodb.org/browse/KAFKA-378) Changed connection uri configuration to password type and for security removed the legacy partition map.
  - [KAFKA-360](https://jira.mongodb.org/browse/KAFKA-360) Fixed tombstones on delete by using the `documentKey` if available by default.

## 1.10.0

### Improvements
  - [KAFKA-344](https://jira.mongodb.org/browse/KAFKA-344) Added connector name to JMX metrics
  - [KAFKA-348](https://jira.mongodb.org/browse/KAFKA-348) Added support for SSL via configuration options
  - [KAFKA-322](https://jira.mongodb.org/browse/KAFKA-322) Ensure configs from config providers are always parsed before validation

## 1.9.1

### Bug Fixes
  - [KAFKA-349](https://jira.mongodb.org/browse/KAFKA-349) Further improvements to schema inference for documents nested in arrays.

## 1.9.0

### Improvements
  - [KAFKA-331](https://jira.mongodb.org/browse/KAFKA-331) Report all exceptions to the DLQ when `mongo.errors.tolerance` is `"all"`
  - [KAFKA-308](https://jira.mongodb.org/browse/KAFKA-308) Add the new `change.stream.full.document.before.change` config property
  - [KAFKA-330](https://jira.mongodb.org/browse/KAFKA-330) Introduced `startup.mode = timestamp` that by default
    corresponds to the default source connector behavior and actuates the new configuration property
    `startup.mode.timestamp.start.at.operation.time` that allows users to configure `startAtOperationTime` on the change stream.
    Deprecated the `copy.existing` property; deprecated properties are overridden by normal ones if there is a conflict.
    `startup.mode = copy_existing` and `startup.mode.copy.existing.*` properties should be used instead of
    `copy.existing = true` and `copy.existing.*` properties.
  - [KAFKA-312](https://jira.mongodb.org/browse/KAFKA-312) Add the new `publish.full.document.only.tombstone.on.delete` config
  - [KAFKA-343](https://jira.mongodb.org/browse/KAFKA-343) Improve schema inference for documents nested in arrays


### Bug Fixes
  - [KAFKA-337](https://jira.mongodb.org/browse/KAFKA-337) Fix case sensitive suggestions for enum recommender.
  - [KAFKA-301](https://jira.mongodb.org/browse/KAFKA-301) ByteArrayRecordConverter now returns mutable documents, so post processing can occur.

## 1.8.1

### Bug Fixes
  - [KAFKA-335](https://jira.mongodb.org/browse/KAFKA-335) Use type Attribute for attributes in JMX mbeans


## 1.8.0

### Improvements
  - [KAFKA-304](https://jira.mongodb.org/browse/KAFKA-304) Monitoring and troubleshooting Kafka Connector improvements including JMX support. [Documentation](https://www.mongodb.com/docs/kafka-connector/current/monitoring/)
  - [KAFKA-300](https://jira.mongodb.org/browse/KAFKA-300) Support DBZ change stream events in the CDC.
  - [KAFKA-329](https://jira.mongodb.org/browse/KAFKA-329) Update java driver dependency to 4.7.0+
  - [KAFKA-328](https://jira.mongodb.org/browse/KAFKA-328) Fix SLF4J usage issues


## 1.7.0

### Improvements
  - [KAFKA-279](https://jira.mongodb.org/browse/KAFKA-279) Updated MongoDB Java driver dependency to 4.5.0.
  - [KAFKA-257](https://jira.mongodb.org/browse/KAFKA-257) Improved reporting to sink connector's DLQ; see the documentation of the
    [`errors.deadletterqueue.context.headers.enable`](https://docs.mongodb.com/kafka-connector/current/sink-connector/configuration-properties/error-handling/)
    configuration property for more details.
    Started to rely on retries in the MongoDB Java driver and stopped supporting the configuration properties
    `max.num.retries`, `retries.defer.timeout`. If you have `retryWrites=false` specified in the `connection.uri` configuration property,
    then retries are disabled for the sink connector; remove `retryWrites=false` from `connection.uri` if you want to enable retries.
  - [KAFKA-253](https://jira.mongodb.org/browse/KAFKA-253) Added support for
    [unordered](https://mongodb.github.io/mongo-java-driver/4.3/apidocs/mongodb-driver-core/com/mongodb/client/model/BulkWriteOptions.html#ordered(boolean))
    [bulk writes](https://mongodb.github.io/mongo-java-driver/4.3/apidocs/mongodb-driver-sync/com/mongodb/client/MongoCollection.html#bulkWrite(java.util.List,com.mongodb.client.model.BulkWriteOptions))
    via the new
    [`bulk.write.ordered`](https://docs.mongodb.com/kafka-connector/current/sink-connector/configuration-properties/connector-message/)
    configuration property.
  - [KAFKA-265](https://jira.mongodb.org/browse/KAFKA-265) Added support for
     [allowDiskUse](https://mongodb.github.io/mongo-java-driver/4.4/apidocs/mongodb-driver-sync/com/mongodb/client/AggregateIterable.html#allowDiskUse(java.lang.Boolean))
    when copying existing data via the new
    [`copy.existing.allow.disk.use`](https://docs.mongodb.com/kafka-connector/current/source-connector/configuration-properties/copy-existing/)
    configuration property.

## 1.6.1

### Bug Fixes
  - [KAFKA-238](https://jira.mongodb.org/browse/KAFKA-238) Fix connection validator user privilege check
  - [KAFKA-245](https://jira.mongodb.org/browse/KAFKA-245) Fix UuidProvidedIn[Key|Value]Strategy classes.
  - [KAFKA-244](https://jira.mongodb.org/browse/KAFKA-244) Update java driver dependency to 4.3.1 in the combined jars]

## 1.6.0

### Improvements
  - [KAFKA-237](https://jira.mongodb.org/browse/KAFKA-237) Improved error logging for bulk write exceptions.
  - [KAFKA-181](https://jira.mongodb.org/browse/KAFKA-181) Added support for serverApi.
  - [KAFKA-228](https://jira.mongodb.org/browse/KAFKA-228) Added support for automatic timeseries collection creation for 5.0
  - [KAFKA-215](https://jira.mongodb.org/browse/KAFKA-215) Added mongo specific override options for error handling properties
  - [KAFKA-222](https://jira.mongodb.org/browse/KAFKA-222) Added a new jar `mongo-kafka-connect-<version>-confluent.jar` which just contains
    the dependencies needed for running the connector with confluent. `mongo-kafka-connect-<version>-all.jar` now also includes `Avro`
    dependencies for ease of deployment for alternative Kafka connect runtimes.

### Bug Fixes
  - [KAFKA-218](https://jira.mongodb.org/browse/KAFKA-218) Fixed bug in LazyBsonDocument#clone ignoring any changes made once unwrapped.
  - [KAFKA-220](https://jira.mongodb.org/browse/KAFKA-220) Fixed bug with timestamp integer overflow.
  - [KAFKA-219](https://jira.mongodb.org/browse/KAFKA-219) Fixed Source Connector unable to recover from broken change stream due to event > 16MB
  - [KAFKA-230](https://jira.mongodb.org/browse/KAFKA-230) Fixed Source Connector recovery during getMore call

## 1.5.1

### Bug Fixes
  - [KAFKA-227](https://jira.mongodb.org/browse/KAFKA-195) Fixed wrapping nullable value returned from WriteModelStrategy
  - [KAFKA-217](https://jira.mongodb.org/browse/KAFKA-217) Fixed using `ns` field in `copy.existing` pipeline bug

## 1.5.0

### Improvements
  - [KAFKA-213](https://jira.mongodb.org/browse/KAFKA-213) Updated MongoDB Java Driver to 4.2.
  - [KAFKA-168](https://jira.mongodb.org/browse/KAFKA-168) Added `DeleteOneBusinessKeyStrategy` for topics containing records to removed from MongoDB.
  - [KAFKA-183](https://jira.mongodb.org/browse/KAFKA-183) Added support for the errant record reporter if available.
  - [KAFKA-205](https://jira.mongodb.org/browse/KAFKA-205) Updated Source connector to use RawBsonDocuments.
  - [KAFKA-201](https://jira.mongodb.org/browse/KAFKA-201) Improved `copy.existing` namespace handling.
  - [KAFKA-207](https://jira.mongodb.org/browse/KAFKA-207) Improved efficiency of heartbeats by making them tombstone messages.
  - [KAFKA-174](https://jira.mongodb.org/browse/KAFKA-174) Improved error messages when using invalid pipeline operators.
  - [KAFKA-194](https://jira.mongodb.org/browse/KAFKA-194) Added support for Qlik Replicate CDC.

### Bug Fixes
  - [KAFKA-195](https://jira.mongodb.org/browse/KAFKA-195) Fixed `topics.regex` sink validation issue for synthetic config property
  - [KAFKA-203](https://jira.mongodb.org/browse/KAFKA-203) Fixed sink NPE issue when using with confluent connect 6.1.0
  - [KAFKA-209](https://jira.mongodb.org/browse/KAFKA-209) Fixed `_id` always being projected even if not explicitly allowed or blocked.
    Log a warning message when there the `_id` value and the id strategy is configured not to overwrite the `_id`.
  - [KAFKA-210](https://jira.mongodb.org/browse/KAFKA-210) Fix inferred schema naming convention and ensure schemas can be backwards compatible.
  - [KAFKA-212](https://jira.mongodb.org/browse/KAFKA-212) Ensure closing the change stream cursor doesn't leak any errors.

## 1.4.0

### Improvements
  - [KAFKA-167](https://jira.mongodb.org/browse/KAFKA-167) Updated MongoDB Java Driver to 4.1.
  - [KAFKA-51](https://jira.mongodb.org/browse/KAFKA-51) Added sink support for MongoDB Changestream events.
  - [KAFKA-159](https://jira.mongodb.org/browse/KAFKA-159) Added dynamic namespace mapping for the sink connector.
  - [KAFKA-185](https://jira.mongodb.org/browse/KAFKA-185) Added topic mapping for the source connector.

### Bug Fixes
  - [KAFKA-171](https://jira.mongodb.org/browse/KAFKA-171) Fixed bug which made the top level inferred schema optional
  - [KAFKA-166](https://jira.mongodb.org/browse/KAFKA-166) Fixed sink validation issue including synthetic config property
  - [KAFKA-180](https://jira.mongodb.org/browse/KAFKA-180) Fix LazyBsonDocument clone, no need to try and unwrap the values before cloning.
  - [KAFKA-188](https://jira.mongodb.org/browse/KAFKA-188) Fix logging of general exceptions.

## 1.3.0
  - [KAFKA-129](https://jira.mongodb.org/browse/KAFKA-129) Added support for Bson bytes in the Sink connector.
  - [KAFKA-122](https://jira.mongodb.org/browse/KAFKA-122) Added support for creating Bson bytes data in the Source connector.
  - [KAFKA-99](https://jira.mongodb.org/browse/KAFKA-99) Added support for custom Json formatting.
  - [KAFKA-132](https://jira.mongodb.org/browse/KAFKA-132) Don't try to publish a source record without a topic name.
  - [KAFKA-133](https://jira.mongodb.org/browse/KAFKA-133) Test against the latest Kafka and Confluent versions.
  - [KAFKA-136](https://jira.mongodb.org/browse/KAFKA-136) Fixed 3.6 copy existing issue when collection doesn't exist.
  - [KAFKA-124](https://jira.mongodb.org/browse/KAFKA-124) Added schema support for the source connector.
  - [KAFKA-137](https://jira.mongodb.org/browse/KAFKA-137) Support dotted field lookups when using schemas.
  - [KAFKA-128](https://jira.mongodb.org/browse/KAFKA-128) Sanitized the connection string in the partition map.
  - [KAFKA-145](https://jira.mongodb.org/browse/KAFKA-145) Ensure the fullDocument field is a document.
  - [KAFKA-125](https://jira.mongodb.org/browse/KAFKA-125) Added infer schema value support for the source connector.
  - [KAFKA-131](https://jira.mongodb.org/browse/KAFKA-131) Added `copy.existing.pipeline` configuration.
    Note: Allows indexes to be used during the copying process, use when there is any filtering done by the main pipeline.
  - [KAFKA-146](https://jira.mongodb.org/browse/KAFKA-146) Improve error handling and messaging for list configuration options.
  - [KAFKA-154](https://jira.mongodb.org/browse/KAFKA-154) Improve the handling and error messaging for Json array config values.
  - [KAFKA-78](https://jira.mongodb.org/browse/KAFKA-78) Added dead letter queue support for the source connector.
  - [KAFKA-157](https://jira.mongodb.org/browse/KAFKA-157) Improved error message for business key errors.
  - [KAFKA-155](https://jira.mongodb.org/browse/KAFKA-155) Fix business key update strategies to use dot notation for filters
  - [KAFKA-105](https://jira.mongodb.org/browse/KAFKA-105) Improve `errors.tolerance=all` support in the sink and source connectors.
  - [KAFKA-106](https://jira.mongodb.org/browse/KAFKA-106) Changed `max.num.retries` default to 1. A safer default especially as the driver now has retryable writes.
  - [KAFKA-147](https://jira.mongodb.org/browse/KAFKA-147) Added `copy.existing.namespace.regex` configuration, that allows the filtering of namespaces to be copied.
  - [KAFKA-158](https://jira.mongodb.org/browse/KAFKA-158) Added `offset.partition.name` configuration, which allows for custom partitioning naming strategies.
    Note: This can be used to start a new change stream, when an existing offset contains an invalid resume token.

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
