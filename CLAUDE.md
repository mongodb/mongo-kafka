# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Official MongoDB Kafka Connector — a Kafka Connect plugin that ships both a **sink** connector (Kafka → MongoDB) and a **source** connector (MongoDB change streams → Kafka). Published as `org.mongodb.kafka:mongo-kafka-connect` on Maven Central; also packaged as a Confluent Hub archive.

## Build & test

Gradle wrapper, Kotlin DSL (`build.gradle.kts`). Java 8 bytecode (`options.release = 8`) compiled by JDK 17 toolchain — do not use Java 9+ APIs in `src/main`.

```bash
./gradlew check                               # compile + spotless + checkstyle + spotbugs + unit tests
./gradlew test                                # unit tests only
./gradlew integrationTest -Dorg.mongodb.test.uri=mongodb://localhost:27017/?replicaSet=rs0
./gradlew test --tests "com.mongodb.kafka.connect.sink.MongoSinkTaskTest"
./gradlew test --tests "*MongoSinkTaskTest.testSomething"
./gradlew spotlessApply                       # auto-format (also runs as compileJava dep)
./gradlew checkstyleMain spotbugsMain         # static checks only
./gradlew confluentJar createConfluentArchive # Confluent Hub artifact
./gradlew compileBuildConfig                  # generates Versions.java if IntelliJ complains
```

Integration tests **require a running MongoDB replica set** (source connector needs change streams). The URI is passed via `-Dorg.mongodb.test.uri=...`; without it the integration suite will fail to connect. Unit tests do not need MongoDB.

To run with a different JDK: `./gradlew -PjavaVersion=11 test` (the toolchain auto-provisions).

`spotlessApply` runs automatically before `compileJava`, so formatting drift will be silently fixed on build — be aware when reviewing diffs after a build.

## Architecture

Two independent connectors share `util/` helpers but otherwise live in separate trees.

### Sink (`src/main/java/com/mongodb/kafka/connect/sink/`)

`MongoSinkConnector` → `MongoSinkTask` → `StartedMongoSinkTask`. Per-topic config lives in `MongoSinkTopicConfig`; the task fans records out by topic. The processing pipeline for each batch:

1. **Convert**: `converter/` turns the Kafka `SinkRecord` into a `BsonDocument` (key + value).
2. **Post-process**: chain of `processor/PostProcessor`s — projections (allow/block-list), `DocumentIdAdder` (assigns `_id` via `id/IdStrategy`), `KafkaMetaAdder`, etc.
3. **CDC handling** (optional): if a `CdcHandler` is configured (`cdc/debezium/`, `cdc/mongodb/`, `cdc/qlik/`), the post-processed doc is interpreted as a CDC event and translated into a `WriteModel`.
4. **Write model**: `writemodel/strategy/WriteModelStrategy` chooses the Mongo write op (`InsertOneDefaultStrategy`, `ReplaceOneBusinessKeyStrategy`, `UpdateOneTimestampsStrategy`, etc.) and emits a `WriteModel<BsonDocument>` for `bulkWrite`.
5. **Errors**: `dlq/` integrates with Kafka Connect's dead-letter queue; `MongoProcessedSinkRecordData` carries the per-record outcome.

`namespace/` resolves the target `db.collection` (supports topic→namespace mapping). `RateLimitSettings` and `RecordBatches` control throughput. `Configurable` is the marker for user-pluggable extension classes loaded via `ClassHelper`.

### Source (`src/main/java/com/mongodb/kafka/connect/source/`)

`MongoSourceConnector` → `MongoSourceTask` → `StartedMongoSourceTask`. Tails a MongoDB **change stream** (cluster/db/collection scope) and emits Kafka `SourceRecord`s. Key pieces:

- `MongoCopyDataManager`: optional initial `copy.existing` snapshot phase before tailing the stream.
- `producer/SchemaAndValueProducer`: pluggable record shape — `BsonSchemaAndValueProducer` (raw BSON), `AvroSchemaAndValueProducer` (with `schema/AvroSchema` from a user-supplied schema), `InferSchemaAndValueProducer` (derives a Connect `Schema` from the doc), `RawJsonStringSchemaAndValueProducer`.
- `topic/mapping/`: derives the destination Kafka topic from the change event's namespace.
- `heartbeat/`: emits heartbeat records so the resume token advances on idle namespaces.
- `json/`: BSON ↔ extended-JSON conversion used by the Avro/JSON producers.
- `statistics/` + `util/jmx/SourceTaskStatistics`: JMX metrics surface.

Resume tokens are persisted via Kafka Connect's source offset machinery; `util/ResumeTokenUtils` handles parsing/validation.

### Cross-cutting util (`src/main/java/com/mongodb/kafka/connect/util/`)

- `ConnectionValidator` + `ServerApiConfig`: pre-flight checks invoked from `validate()` on both connectors (verifies connectivity and that the user has the required actions — `insert/update/remove[/collStats]` for sink, `changeStream/find` for source).
- `custom/credentials/`: SPI for user-supplied auth providers (e.g. AWS IAM / Assume Role). See `util/custom/credentials/README.md`.
- `config/ConfigSoftValidator`: emits warnings for deprecated/incompatible config combos without failing startup. Both connectors have a `*ConfigSoftValidator` that uses it.
- `jmx/`: per-task JMX metric beans.
- `Versions.java` is **generated** at build time by the `buildconfig` plugin from `git describe`. If IntelliJ reports `cannot find symbol: Versions`, run `./gradlew compileBuildConfig`.

### Packaging

Three jars are produced via the Shadow plugin:

- `mongo-kafka-connect-<ver>.jar` — slim (no shaded deps), the Maven `default` artifact.
- `…-confluent.jar` — bundles the Mongo driver, `mongodb-crypt`, and Avro for Confluent Hub.
- `…-all.jar` — same shading, distributed alongside Maven publish.

The `dependencies` block in `build.gradle.kts` carries several `TODO: Remove this override` security pins (CVE patches for transitive deps via Checkstyle, SpotBugs, Avro, and Kafka). When bumping those upstream versions, re-check whether the constraint is still needed and remove the TODO comment.

## Conventions

- Spotless config: Google Java Format 1.12.0, custom import order `java, io, org, org.bson, com.mongodb, com.mongodb.kafka, ""`. Don't reorder imports manually; let Spotless do it.
- Checkstyle config: `config/checkstyle/`. Spotbugs exclusions: `config/spotbugs-exclude.xml`.
- Tests use JUnit 5 (Jupiter) + Mockito + Hamcrest. Integration tests use Confluent's embedded Kafka (`org.apache.kafka:kafka-streams::test`) and `kafka-schema-registry`.
- Branch names and commits use `KAFKA-<n>` JIRA prefixes (issues live at https://jira.mongodb.org/browse/KAFKA).
- Releases are tag-driven: `publishArchives` refuses to run if the project version doesn't match `git describe`.
