# MongoDB Kafka Connector

## Installation guide

The easiest way to install the connector is via the confluent hub. See [The install documentation]|https://www.confluent.io/hub/mongodb/kafka-connect-mongodb]
in the confluent hub for more information.

The connector is available from either the [oss sonatype snapshot repo](https://oss.sonatype.org/content/repositories/snapshots/org/mongodb/kafka/mongo-kafka-connect/) 
or [maven central](https://search.maven.org/search?q=g:org.mongodb.kafka%20AND%20a:mongo-kafka-connect).

The uber jar (`mongo-kafka-connect-0.2-all.jar`) contains all the dependencies required for the connector.

To install:

  - On the Kafka Connector machine copy the uber jar into the plugins directory: `/usr/local/share/kafka/plugins/`
  - Configure the connector.
    * The [`MongoSinkConnector.properties`](../config/MongoSinkConnector.properties) provides an example configuration for the MongoDB Kafka Sink connector.
      Where data is copied from one or more topics into MongoDB.
    * The [`MongoSourceConnector.properties`](../config/MongoSourceConnector.properties) provides an example configuration for the MongoDB Kafka Source connector.
      Where change stream data is published from MongoDB onto Kafka topics
    

For more information on installing connectors see the official Confluent documentation.

  - [Manually installing community connectors](https://docs.confluent.io/5.2.2/connect/managing/community.html) and 
  - [Configuring connectors](https://docs.confluent.io/5.2.2/connect/managing/configuring.html) for more information.


## Migration guide

How to migrate from Kafka Connect MongoDB / hpgrahsl/kafka-connect-mongodb.

  - Package name: `at.grahsl.kafka.connect.mongodb` becomes `com.mongodb.kafka.connect`
  - Naming convention:
    - `MongoDbSinkConnector` becomes `MongoSinkConnector`
    - `MongoDbSinkConnectorConfig` has been split into: `sink.MongoSinkConfig` & `sink.MongoSinkTopicConfig`.
  - Configuration:
    - Removed `mongodb.` prefix on all configurations.
    - Removed `mongodb.collections`, per topic/collection overrides have been simplified.<br> 
      See [topic specific configuration settings](./sink.md#topic-specific-configuration-settings) for more information.
    - Removed `document.id.strategies`. No longer needed, users can just implement the 
      `com.mongodb.kafka.connect.sink.processor.id.strategy.IdStrategy` interface.
  - PostProcessor:
     - **Breaking changes.**
       The abstract class has been redefined and the method parameters have changed which configuration they take.
       Post Processors are now designed to be immutable and self contained.

---
### Next

- Installation guide
- [The MongoDB Kafka sink connector guide](./sink.md)
- [The MongoDB Kafka source connector guide](./source.md)
- [A docker end 2 end example](../docker/README.md)
- [Changelog](./changelog.md)
