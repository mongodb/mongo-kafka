#Kafka Connect MongoDB

[![Build Status](https://travis-ci.org/hpgrahsl/kafka-connect-mongodb.svg?branch=master)](https://travis-ci.org/hpgrahsl/kafka-connect-mongodb) [![Codacy Badge](https://api.codacy.com/project/badge/Coverage/9ce80f1868154f02ad839eb76521d582)](https://www.codacy.com/app/hpgrahsl/kafka-connect-mongodb?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=hpgrahsl/kafka-connect-mongodb&amp;utm_campaign=Badge_Coverage)

It's a basic [Apache Kafka](https://kafka.apache.org/) [Connect SinkConnector](https://kafka.apache.org/documentation/#connect) for [MongoDB](https://www.mongodb.com/).
The connector uses the official MongoDB [Java Driver](http://mongodb.github.io/mongo-java-driver/3.4/driver/).
Future releases might additionally support the [asynchronous driver](http://mongodb.github.io/mongo-java-driver/3.4/driver-async/]).

### Supported Sink Record Structure
Currently the connector is able to process Kafka Connect SinkRecords with
support for the following schema types [Schema.Type](https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Schema.Type.html):
*INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT*.

The conversion is able to generically deal with nested key or value structures - based on the supported types above - like the following example which is based on [AVRO](https://avro.apache.org/)

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

##### Logical Types
Besides the standard types it is possible to use logical types in order to have field type support for

* **Decimal**
* **Date**
* **Timestamp**
* **Time**

For obvious reasons, logical types can only be supported for **AVRO** and **JSON + Schema** data (see section below). When using AVRO some Kafka (Connect) induced pecularities apply. The following example based on an exemplary definition of the **Decimal** logical type should make this clearer:

While a standard AVRO schema would as follows:

```json
{
  "type": "bytes",
  "logicalType": "decimal",
  "scale": 2
}
```

You have to write it like this when using kafka connect:

```json
{
  "type": "bytes",
  "connect.version": 1,
  "connect.parameters": {
    "scale": "2"
  },
  "connect.name": "org.apache.kafka.connect.data.Decimal"
}
```


### Supported Data Formats
The sink connector implementation is configurable in order to support

* **AVRO** (makes use of Confluent's Kafka Schema Registry and is the recommended format)
* **JSON with Schema** (offers JSON record structure with explicit schema information)
* **JSON plain** (offers JSON record structure without any attached schema)
* **RAW JSON** (string only - JSON structure not managed by Kafka connect)

Since these settings can be independently configured, it's possible to have different settings for the key and value of record respectively.

##### Configuration example for AVRO
```properties
key.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://localhost:8081

value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://localhost:8081
```

##### Configuration example for JSON with Schema
```properties
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true

value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
```

### Key Handling Strategies
The sink connector is able to process both, the key and value parts of kafka records. After the conversion to MongoDB BSON documents, an *_id* field is automatically added to value documents which are finally persisted in a MongoDB collection. The *_id* itself is filled by the **configured document id generation strategy**, which can be one of the following:

* a MongoDB **BSON ObjectId**
* a Java **UUID**
* **Kafka meta-data** comprised of the string concatenation based on [topic-partition-offset] information
* **full key** using the sink record's complete key structure
* **provided in key** expects the sink record's key to contain an *_id* field which is used as is (error if not present or null)
* **provided in value** expects the sink record's value to contain an *_id* field which is used as is (error if not present or null)
* **partial key** using parts of the sink record's key structure 
* **partial value** using parts of the sink record's value structure

_Note: the latter two of which can be configured to use the blacklist/whitelist field projection mechanisms described below._

It's important to keep in mind that the chosen **id strategy** has direct implications on the possible delivery semantics. Obviously, if it's set to BSON ObjectId or UUID respectively, it can only guarantee at-most-once delivery of records, since new ids will result due to the re-processing on retries after failures. The other strategies permit exactly-once semantics iff the respective fields forming the document *_id* are guaranteed to be unique in the first place.

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
```properties
mongodb.field.projection.type=blacklist
mongodb.field.projection.list=age,address.city,lut.key2
```

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
```properties
mongodb.field.projection.type=whitelist
mongodb.field.projection.list=age,address.city,lut.key2
```

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

```properties
mongodb.field.projection.type=whitelist
mongodb.field.projection.list=age,lut.*
```

-> will include: the *age* field, the *lut* field and all its immediate subfiels (i.e. one level down)

Example 2: 

```properties
mongodb.field.projection.type=whitelist
mongodb.field.projection.list=active,address.**
```

-> will include: the *active* field, the *address* field and its full sub structure (all available nested levels)

Example 3:

```properties
* mongodb.field.projection.type=whitelist
* mongodb.field.projection.list=*.*
```

-> will include: all fields on the 1st and 2nd level

#####Blacklist examples:
Example 1:

```properties
* mongodb.field.projection.type=blacklist
* mongodb.field.projection.list=age,lut.*
```

-> will exclude: the *age* field, the *lut* field and all its immediate subfields (i.e. one level down)

Example 2: 

```properties
mongodb.field.projection.type=blacklist
mongodb.field.projection.list=active,address.**
```

-> will exclude: the *active* field, the *address* field and its full sub structure (all available nested levels)

Example 3:

```properties
mongodb.field.projection.type=blacklist
mongodb.field.projection.list=*.*
```

-> will exclude: all fields on the 1st and 2nd level

### Post Processors
Right after the conversion, the BSON documents undergo a **chain of post processors**. There are the following 3 active processors at the moment:

* **DocumentIdAdder** (mandatory): uses the configured _strategy_ (see above) to insert an **_id field**
* **BlacklistProjector** (optional): applicable for _key_ + _value_ structure
* **WhitelistProjector** (optional): applicable for _key_ + _value_ structure

Further post processor could be easily implemented and added to the chain, like:

* add kafka meta data (topic,partition,offset)
* rename arbitrary fields
* remove fields with null values
* etc. 

What's still missing is a configurable way to dynamically build the chain.

### MongoDB Persistence
The sink records are converted to BSON documents which are in turn inserted into the corresponding MongoDB target collection. The implementation uses unorderd bulk writes based on the [ReplaceOneModel](http://mongodb.github.io/mongo-java-driver/3.4/javadoc/com/mongodb/client/model/ReplaceOneModel.html) together with [upsert mode](http://mongodb.github.io/mongo-java-driver/3.4/javadoc/com/mongodb/client/model/UpdateOptions.html).

Data is written using acknowledged writes and the configured write concern level. If the bulk write fails (totally or partially) errors are logged and a simple retry logic is in place. More robust/sophisticated failure mode handling has yet to be implemented.
 
### Sink Connector Properties 

At the moment the following settings can be configured by means of the *connector.properties* file

| Name                          | Description                                              | Type     | Default      | Valid Values                                                   | Importance |
|-------------------------------|----------------------------------------------------------|----------|--------------|----------------------------------------------------------------|------------|
| mongodb.collection            | single sink collection name to write to                  | string   | kafkatopic   |                                                                | high       |
| mongodb.database              | sink database name to write to                           | string   | kafkaconnect |                                                                | high       |
| mongodb.document.id.strategy  | which strategy to use for a unique document id (_id)     | string   | objectid     | [objectid, uuid, kafkameta, fullkey, partialkey, partialvalue, providedinkey, providedinvalue] | high       |
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
