#Kafka Connect MongoDB

It's a basic Apache Kafka Connect SinkConnector for MongoDB.
The connector uses the official MongoDB [Java Driver](http://mongodb.github.io/mongo-java-driver/3.4/driver/).
Future releases might additionally support the [asynchronous driver](http://mongodb.github.io/mongo-java-driver/3.4/driver-async/]).

### Supported Record Structure
Currently the connector is able to process Kafka Connect SinkRecords with
support for the following schema types [Schema.Type](https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Schema.Type.html):
*INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT*.

The conversion is able to generically deal with nested key or value structures - based on the supported types above - like the following example:

```json
{"type": "record",
  "name": "Customer",
  "namespace": "at.grahsl.data.kafka.avro",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "active", "type": "boolean"},
    {"name": "address", "type":
    {"type": "record",
      "name": "AddressRecord",
      "fields": [
        {"name": "city", "type": "string"},
        {"name": "country", "type": "string"}
      ]}
    },
    {"name": "food", "type": {"type": "array", "items": "string"}},
    {"name": "data", "type": {"type": "array", "items":
    {"type": "record",
      "name": "Whatever",
      "fields": [
        {"name": "k", "type": "string"},
        {"name": "v", "type": "int"}
      ]}
    }},
    {"name": "lut", "type": {"type": "map", "values": "double"}},
    {"name": "raw", "type": "bytes"}
  ]
}
```

### Key Handling Strategies
So far, the sink connector ignores the key part of the kafka records and only processes their respective value structures.
After the conversion to a MongoDB BSON document, an *_id* field holding a MongoDB ObjectID() is automatically added.
This essentially supports 'insert only' workloads where each document is guaranteed to get its unique id in the sink
(i.e. MongoDB collection) irrespective of the key in the source record. While this may fit special use cases,
it will certainly forbid others.

Thus, it is planned for the connector to support other key handling strategies such as:

* full record key: uses the sink record's complete key structure as _id field of the MongoDB document
* partial record key: uses single field of the sink record's key structure as _id field of the MongoDB document
* partial record value: uses single field of the sink record's value structure as _id field of the MongoDB document
* kafka meta data: uses the string concatenation of the kafka topic+partition+offset as _id field of the MongoDB document

These key strategies in combination with corresponding config settings will eventually allow for both,
upsert driven workloads or stronger delivery semantics at the sink side.

### Value Handling Strategies
The current implementation converts and persists the full value structure of the sink records.
To have more flexibility in this regard there should be future support for:

* explicit null handling: the option to preserve / ignore fields with null values
* white-/blacklist 'projection': keep/remove all the listed fields from the value structure

### MongoDB Persistence
The collection of sink records is converted to BSON documents which are in turn inserted using a bulk write operation.
Data is saved with the given write concern level and any errors reported back are currently only logged by inspecting
the BulkWriteResult object.
 
### Sink Connector Properties 

At the moment the following settings can be configured by means of the *connector.properties* file

| Name                 | Description                                        | Type     | Default      | Valid Values  | Importance |
|----------------------|----------------------------------------------------|----------|--------------|---------------|------------|
| mongodb.collection   | single sink collection name to write to            | string   | kafkatopic   |               | high       |
| mongodb.database     | sink database name to write to                     | string   | kafkaconnect |               | high       |
| mongodb.host         | single mongod host to connect with                 | string   | localhost    |               | high       |
| mongodb.port         | port mongod is listening on                        | int      | 27017        | [0,...,65536] | high       |
| mongodb.writeconcern | write concern to apply when saving data            | string   | 1            |               | high       |
| mongodb.auth.active  | whether or not the connection needs authentication | boolean  | false        |               | medium     |
| mongodb.auth.db      | authentication database to use                     | string   | admin        |               | medium     |
| mongodb.auth.mode    | which authentication mechanism is used             | string   | SCRAM-SHA-1  | [SCRAM-SHA-1] | medium     |
| mongodb.password     | password for authentication                        | password | [hidden]     |               | medium     |
| mongodb.username     | username for authentication                        | string   | ""           |               | medium     |

In addition the planned features mentioned in the sections above the following enhancements would be beneficial:

* SSL connection support
* further authentication mechanisms
* other client options w.r.t the driver connection
* multiple sink collections to process several topics

### Running in development

```
mvn clean package
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/MongoDbSinkConnector.properties
```
