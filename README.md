#Kafka Connect MongoDB

It's a basic [Apache Kafka](https://kafka.apache.org/) [Connect SinkConnector](https://kafka.apache.org/documentation/#connect) for [MongoDB](https://www.mongodb.com/).
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
The sink connector is able to process both, the key and value parts of kafka records. After the conversion to MongoDB BSON documents, an *_id* field is automatically added to value documents which are finally persisted in a MongoDB collection. The *_id* itself is filled by the **configured document id generation strategy**, which can be one of the following:

* a MongoDB **BSON ObjectId**
* a Java **UUID**
* **Kafka meta-data** comprised of the string concatenation based on [topic-partition-offset] information
* **full key** using the sink record's complete key structure
* **partial key** using parts of the sink record's key structure 
* **partial value** using parts of the sink record's value structure

_Note: the latter two of which can be configured to use the blacklist/whitelist field projection mechanisms described below._

These key handling strategies combined with proper error handling and retry mechanisms will allow to support different use cases ranging from insert-only (at least once) to upsert driven (exactly/effectively once) delivery semantics towards a MongoDB collection. However, the implementation for these delivery semantics is still in progress...

### Value Handling Strategies
By default the current implementation converts and persists the full value structure of the sink records.
Value handling can be configured by using either a **blacklist or whitelist** approach in order to remove/keep fields
from the value structure. By using the "." notation to access sub documents it's also supported to do 
redaction of nested fields. See two concrete examples below about the behaviour of these two projection strategies

Given the following fictional data record:

```json
{ "name": "Anonymous", 
  "age": 42,
  "active": true, 
  "address": {"city": "Unknown", "country": "NoWhereLand"},
  "food": ["Austrian", "Italian"],
  "data": [{"k": "foo", "v": 1}],
  "lut": {"key1": 12.34, "key2": 23.45}
}
```

#####Example blacklist projection:

* mongodb.field.projection.type=**blacklist**
* mongodb.field.projection.list=_age,address.city,lut.key2_

will result in:

```json
{ "name": "Anonymous", 
  "active": true, 
  "address": {"country": "NoWhereLand"},
  "food": ["Austrian", "Italian"],
  "data": [{"k": "foo", "v": 1}],
  "lut": {"key1": 12.34}
}
```

#####Example whitelist projection:

* mongodb.field.projection.type=**whitelist**
* mongodb.field.projection.list=_age,address.city,lut.key2_

will result in:

```json
{ "age": 42, 
  "address": {"city": "Unknown"},
  "lut": {"key2": 23.45}
}
```

To have more flexibility in this regard there might be future support for:

* explicit null handling: the option to preserve / ignore fields with null values
* investigate if it makes sense to support array element access for field projections

### How wildcard pattern matching works:
The configuration supports wildcard matching using a __'\*'__ character notation. A wildcard
is supported on any level in the document structure in order to include (whitelist) or
exclude (blacklist) any fieldname at the corresponding level. A part from that there is support
for __'\*\*'__ which can be used at any level to include/exclude the full sub structure
(i.e. all nested levels further down in the hierarchy).

#####Whitelist examples:

Example 1: 

* mongodb.field.projection.type=whitelist
* mongodb.field.projection.list=age,lut.\*

-> will include: the *age* field, the *lut* field and all its immediate subfiels (i.e. one level down)

Example 2: 

* mongodb.field.projection.type=whitelist
* mongodb.field.projection.list=active,address.\*\*

-> will include: the *active* field, the *address* field and its full sub structure (all available nested levels)

Example 3:

* mongodb.field.projection.type=whitelist
* mongodb.field.projection.list=\*.\*

-> will include: all fields on the 1st and 2nd level

#####Blacklist examples:
Example 1:

* mongodb.field.projection.type=blacklist
* mongodb.field.projection.list=age,lut.*

-> will exclude: the *age* field, the *lut* field and all its immediate subfields (i.e. one level down)

Example 2: 

* mongodb.field.projection.type=blacklist
* mongodb.field.projection.list=active,address.\*\*

-> will exclude: the *active* field, the *address* field and its full sub structure (all available nested levels)

Example 3:

* mongodb.field.projection.type=blacklist
* mongodb.field.projection.list=\*.\*

-> will exclude: all fields on the 1st and 2nd level


### MongoDB Persistence
The collection of sink records is converted to BSON documents which are in turn inserted using a bulk write operation.
Data is saved with the given write concern level and any partial errors reported back are currently only logged by inspecting
the BulkWriteResult object. In case the full bulk write operation fails (e.g. network connection to MongoDB is down) then there is a very simple retry logic in place. More robust failure mode handling has yet to be implemented.
 
### Sink Connector Properties 

At the moment the following settings can be configured by means of the *connector.properties* file

| Name                          | Description                                              | Type     | Default      | Valid Values                                                   | Importance |
|-------------------------------|----------------------------------------------------------|----------|--------------|----------------------------------------------------------------|------------|
| mongodb.collection            | single sink collection name to write to                  | string   | kafkatopic   |                                                                | high       |
| mongodb.database              | sink database name to write to                           | string   | kafkaconnect |                                                                | high       |
| mongodb.document.id.strategy  | which strategy to use for a unique document id (_id)     | string   | objectid     | [objectid, uuid, kafkameta, fullkey, partialkey, partialvalue] | high       |
| mongodb.host                  | single mongod host to connect with                       | string   | localhost    |                                                                | high       |
| mongodb.port                  | port mongod is listening on                              | int      | 27017        | [0,...,65536]                                                  | high       |
| mongodb.writeconcern          | write concern to apply when saving data                  | string   | 1            |                                                                | high       |
| mongodb.auth.active           | whether or not the connection needs authentication       | boolean  | false        |                                                                | medium     |
| mongodb.auth.db               | authentication database to use                           | string   | admin        |                                                                | medium     |
| mongodb.auth.mode             | which authentication mechanism is used                   | string   | SCRAM-SHA-1  | [SCRAM-SHA-1]                                                  | medium     |
| mongodb.max.num.retries       | how often a retry should be done on write errors         | int      | 1            | [0,...]                                                        | medium     |
| mongodb.password              | password for authentication                              | password | [hidden]     |                                                                | medium     |
| mongodb.retries.defer.timeout | how long in ms a retry should get deferred               | int      | 10000        | [0,...]                                                        | medium     |
| mongodb.username              | username for authentication                              | string   | ""           |                                                                | medium     |
| mongodb.key.projection.list   | comma separated list of field names for key projection   | string   | ""           |                                                                | low        |
| mongodb.key.projection.type   | whether or not and which key projection to use           | string   | none         | [none, blacklist, whitelist]                                   | low        |
| mongodb.value.projection.list | comma separated list of field names for value projection | string   | ""           |                                                                | low        |
| mongodb.value.projection.type | whether or not and which value projection to use         | string   | none         | [none, blacklist, whitelist]                                   | low        |


In addition to some planned features mentioned in the sections above the following enhancements would be beneficial:

* SSL connection support
* further authentication mechanisms
* other client options w.r.t the driver connection
* etc.

### Running in development

```
mvn clean package
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/MongoDbSinkConnector.properties
```
