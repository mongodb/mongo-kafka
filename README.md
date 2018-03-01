# Kafka Connect MongoDB

[![Build Status](https://travis-ci.org/hpgrahsl/kafka-connect-mongodb.svg?branch=master)](https://travis-ci.org/hpgrahsl/kafka-connect-mongodb) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/9ce80f1868154f02ad839eb76521d582)](https://www.codacy.com/app/hpgrahsl/kafka-connect-mongodb?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=hpgrahsl/kafka-connect-mongodb&amp;utm_campaign=Badge_Grade) [![Codacy Badge](https://api.codacy.com/project/badge/Coverage/9ce80f1868154f02ad839eb76521d582)](https://www.codacy.com/app/hpgrahsl/kafka-connect-mongodb?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=hpgrahsl/kafka-connect-mongodb&amp;utm_campaign=Badge_Coverage)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=E3P9D3REZXTJS)

It's a basic [Apache Kafka](https://kafka.apache.org/) [Connect SinkConnector](https://kafka.apache.org/documentation/#connect) for [MongoDB](https://www.mongodb.com/).
The connector uses the official MongoDB [Java Driver](http://mongodb.github.io/mongo-java-driver/3.6/).
Future releases might additionally support the [asynchronous driver](http://mongodb.github.io/mongo-java-driver/3.6/driver-async/).

### Supported Sink Record Structure
Currently the connector is able to process Kafka Connect SinkRecords with
support for the following schema types [Schema.Type](https://kafka.apache.org/10/javadoc/org/apache/kafka/connect/data/Schema.Type.html):
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
Besides the standard types it is possible to use [AVRO logical types](http://avro.apache.org/docs/1.8.1/spec.html#Logical+Types) in order to have field type support for

* **Decimal**
* **Date**
* **Time** (millis/micros)
* **Timestamp** (millis/micros)

The following example based on exemplary logical type definitions should make this clearer:

```json
{
  "type": "record",
  "name": "MyLogicalTypesRecord",
  "namespace": "at.grahsl.data.kafka.avro",
  "fields": [
    {
      "name": "myDecimalField",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "connect.parameters": {
          "scale": "2"
        }
      }
    },
    {
      "name": "myDateField",
      "type": {
        "type": "int",
        "logicalType": "date"
      }
    },
    {
      "name": "myTimeMillisField",
      "type": {
        "type": "int",
        "logicalType": "time-millis"
      }
    },
    {
      "name": "myTimeMicrosField",
      "type": {
        "type": "long",
        "logicalType": "time-micros"
      }
    },
    {
      "name": "myTimestampMillisField",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "myTimestampMicrosField",
      "type": {
        "type": "long",
        "logicalType": "timestamp-micros"
      }
    }
  ]
}
```

Note that if you are using AVRO code generation for logical types in order to use them from a Java-based producer app you end-up with the following Java type mappings:

* org.joda.time.LocalDate myDateField;
* org.joda.time.LocalTime mytimeMillisField;
* long myTimeMicrosField;
* org.joda.time.DateTime myTimestampMillisField;
* long myTimestampMicrosField;

See [this discussion](https://github.com/hpgrahsl/kafka-connect-mongodb/issues/5) if you are interested in some more details.

For obvious reasons, logical types can only be supported for **AVRO** and **JSON + Schema** data (see section below).

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

### Post Processors
Right after the conversion, the BSON documents undergo a **chain of post processors**. There are the following 4 processors to choose from:

* **DocumentIdAdder** (mandatory): uses the configured _strategy_ (explained below) to insert an **_id field**
* **BlacklistProjector** (optional): applicable for _key_ + _value_ structure
* **WhitelistProjector** (optional): applicable for _key_ + _value_ structure
* **FieldRenamer** (optional): applicable for _key_ + _value_ structure

Further post processors can be easily implemented based on the provided abstract base class [PostProcessor](https://github.com/hpgrahsl/kafka-connect-mongodb/blob/master/src/main/java/at/grahsl/kafka/connect/mongodb/processor/PostProcessor.java), e.g.

* remove fields with null values
* redact fields containing sensitive information
* etc.

There is a configuration property which allows to customize the post processor chain applied to the converted records before they are written to the sink. Just specify a comma separated list of fully qualified class names which provide the post processor implementations, either existing ones or new/customized ones, like so:

```properties
mongodb.post.processor.chain=at.grahsl.kafka.connect.mongodb.processor.field.renaming.RenameByMapping
```

The DocumentIdAdder is automatically added at the very first position in the chain in case it is not present. Other than that, the chain can be built more or less arbitrarily. However, currently each post processor can only be specified once.

Find below some documentation how to configure the available ones:

##### DocumentIdAdder (mandatory)
The sink connector is able to process both, the key and value parts of kafka records. After the conversion to MongoDB BSON documents, an *_id* field is automatically added to value documents which are finally persisted in a MongoDB collection. The *_id* itself is filled by the **configured document id generation strategy**, which can be one of the following:

* a MongoDB **BSON ObjectId** (default)
* a Java **UUID**
* **Kafka meta-data** comprised of the string concatenation based on [topic-partition-offset] information
* **full key** using the sink record's complete key structure
* **provided in key** expects the sink record's key to contain an *_id* field which is used as is (error if not present or null)
* **provided in value** expects the sink record's value to contain an *_id* field which is used as is (error if not present or null)
* **partial key** using parts of the sink record's key structure 
* **partial value** using parts of the sink record's value structure

_Note: the latter two of which can be configured to use the blacklist/whitelist field projection mechanisms described below._

The strategy is set by means of the following property:

```properties
mongodb.document.id.strategy=at.grahsl.kafka.connect.mongodb.processor.id.strategy.BsonOidStrategy
```

There is a configuration property which allows to customize the applied id generation strategy. Thus, if none of the available strategies fits your needs, further strategies can be easily implemented based on the interface [IdStrategy](https://github.com/hpgrahsl/kafka-connect-mongodb/blob/master/src/main/java/at/grahsl/kafka/connect/mongodb/processor/id/strategy/IdStrategy.java)

All custom strategies that should be available to the connector can be registered by specifying a list of fully qualified class names for the following configuration property:

```properties
mongodb.document.id.strategies=...
```

**It's important to keep in mind that the chosen / implemented id strategy has direct implications on the possible delivery semantics.** Obviously, if it's set to BSON ObjectId or UUID respectively, it can only ever guarantee at-least-once delivery of records, since new ids will result due to the re-processing on retries after failures. The other strategies permit exactly-once semantics iff the respective fields forming the document *_id* are guaranteed to be unique in the first place.

##### Blacklist-/WhitelistProjector (optional)
By default the current implementation converts and persists the full value structure of the sink records.
Key and/or value handling can be configured by using either a **blacklist or whitelist** approach in order to remove/keep fields
from the record's structure. By using the "." notation to access sub documents it's also supported to do 
redaction of nested fields. It is also possible to refer to fields of documents found within arrays by the same notation. See two concrete examples below about the behaviour of these two projection strategies

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

##### Example blacklist projection:
```properties
mongodb.[key|value].projection.type=blacklist
mongodb.[key|value].projection.list=age,address.city,lut.key2,data.v
```

will result in:

```json
{ "name": "Anonymous", 
  "active": true, 
  "address": {"country": "NoWhereLand"},
  "food": ["Austrian", "Italian"],
  "data": [{"k": "foo"}],
  "lut": {"key1": 12.34}
}
```

##### Example whitelist projection:
```properties
mongodb.[key|value].projection.type=whitelist
mongodb.[key|value].projection.list=age,address.city,lut.key2,data.v
```

will result in:

```json
{ "age": 42, 
  "address": {"city": "Unknown"},
  "data": [{"v": 1}],
  "lut": {"key2": 23.45}
}
```

To have more flexibility in this regard there might be future support for:

* explicit null handling: the option to preserve / ignore fields with null values
* investigate if it makes sense to support array element access for field projections based on an index or a given value to project simple/primitive type elements

##### How wildcard pattern matching works:
The configuration supports wildcard matching using a __'\*'__ character notation. A wildcard
is supported on any level in the document structure in order to include (whitelist) or
exclude (blacklist) any fieldname at the corresponding level. A part from that there is support
for __'\*\*'__ which can be used at any level to include/exclude the full sub structure
(i.e. all nested levels further down in the hierarchy).

_NOTE: A bunch of more concrete examples of field projections including wildcard pattern matching can be found in a corresponding [test class](https://github.com/hpgrahsl/kafka-connect-mongodb/blob/master/src/test/java/at/grahsl/kafka/connect/mongodb/processor/field/projection/FieldProjectorTest.java)._

##### Whitelist examples:

Example 1: 

```properties
mongodb.[key|value].projection.type=whitelist
mongodb.[key|value].projection.list=age,lut.*
```

-> will include: the *age* field, the *lut* field and all its immediate subfiels (i.e. one level down)

Example 2: 

```properties
mongodb.[key|value].projection.type=whitelist
mongodb.[key|value].projection.list=active,address.**
```

-> will include: the *active* field, the *address* field and its full sub structure (all available nested levels)

Example 3:

```properties
mongodb.[key|value].projection.type=whitelist
mongodb.[key|value].projection.list=*.*
```

-> will include: all fields on the 1st and 2nd level

##### Blacklist examples:
Example 1:

```properties
mongodb.[key|value].projection.type=blacklist
mongodb.[key|value].projection.list=age,lut.*
```

-> will exclude: the *age* field, the *lut* field and all its immediate subfields (i.e. one level down)

Example 2: 

```properties
mongodb.[key|value].projection.type=blacklist
mongodb.[key|value].projection.list=active,address.**
```

-> will exclude: the *active* field, the *address* field and its full sub structure (all available nested levels)

Example 3:

```properties
mongodb.[key|value].projection.type=blacklist
mongodb.[key|value].projection.list=*.*
```

-> will exclude: all fields on the 1st and 2nd level

##### FieldRenamer (optional)
There are two different options to rename any fields in the record, namely a simple and rigid 1:1 field name mapping or a more flexible approach using regexp. Both config options are defined by inline JSON arrays containing objects which describe the renaming.

Example 1:

```properties
mongodb.field.renamer.mapping=[{"oldName":"key.fieldA","newName":"field1"},{"oldName":"value.xyz","newName":"abc"}]
```

These settings cause:

1) a field named _fieldA_ to be renamed to _field1_ in the **key document structure**
2) a field named _xyz_ to be renamed to _abc_ in the **value document structure**

Example 2:

```properties
mongodb.field.renamer.mapping=[{"regexp":"^key\\..*my.*$","pattern":"my","replace":""},{"regexp":"^value\\..*-.+$","pattern":"-","replace":"_"}]
```

These settings cause:

1) **all field names of the key structure containing 'my'** to be renamed so that **'my' is removed**
2) **all field names of the value structure containing a '-'** to be renamed by replacing **'-' with '_'**

Note the use of the **"." character** as navigational operator in both examples. It's used in order to refer to nested fields in sub documents of the record structure. The prefix at the very beginning is used as a simple convention to distinguish between the _key_ and _value_ structure of a document.

### Change Data Capture Mode
The sink connector can also be used in a different operation mode in order to handle change data capture (CDC) events. Currently, the following CDC events from [Debezium](http://debezium.io/) can be processed:

* [MongoDB](http://debezium.io/docs/connectors/mongodb/) 
* [MySQL](http://debezium.io/docs/connectors/mysql/)
* PostgreSQL (coming later)
* Oracle (not yet finished at Debezium Project)

This effectively allows to replicate all state changes within the source databases into MongoDB collections. Further Debezium formats - namely PostgreSQL and Oracle - will probably get integrated in future releases.
 
Also note that **both serialization formats (JSON+Schema & AVRO) can be used** depending on which configuration is a better fit for your use case.

##### CDC Handler Configuration
The sink connector configuration offers a property called *mongodb.change.data.capture.handler* which is set to the fully qualified class name of the respective CDC format handler class. These classes must extend from the provided abstract class *[CdcHandler](https://github.com/hpgrahsl/kafka-connect-mongodb/blob/master/src/main/java/at/grahsl/kafka/connect/mongodb/cdc/CdcHandler.java)*. As soon as this configuration property is set the connector runs in **CDC operation mode**. Find below a JSON based configuration sample for the sink connector which uses the current default implementation that is capable to process Debezium CDC MongoDB events. This config can be posted to the [Kafka connect REST endpoint](https://docs.confluent.io/current/connect/restapi.html) in order to run the sink connector.

```json
{
  "name": "mdb-sink-debezium-cdc",
  "config": {
    "key.converter":"io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url":"http://localhost:8081",
    "value.converter":"io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url":"http://localhost:8081",
  	"connector.class": "at.grahsl.kafka.connect.mongodb.MongoDbSinkConnector",
    "topics": "myreplset.kafkaconnect.mongosrc",
    "mongodb.connection.uri": "mongodb://mongodb:27017/kafkaconnect?w=1&journal=true",
    "mongodb.change.data.capture.handler": "at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb.MongoDbHandler",
    "mongodb.collection": "mongosink"
  }
}
```

**NOTE:** There are scenarios in which there is no CDC enabled source connector in place. However, it might be required to still be able to handle record deletions. For these cases the sink connector can be configured to delete records in MongoDB whenever it encounters sink records which exhibit _null_ values. This is a simple convention that can be activated by setting the following configuration option:

```properties
mongodb.delete.on.null.values=true
```

Based on this setting the sink connector tries to delete a MongoDB document from the corresponding collection based on the sink record's key or actually the resulting *_id* value thereof, which is generated according to the specified [DocumentIdAdder](https://github.com/hpgrahsl/kafka-connect-mongodb#documentidadder-mandatory).  

### MongoDB Persistence
The sink records are converted to BSON documents which are in turn inserted into the corresponding MongoDB target collection. The implementation uses unorderd bulk writes based on the [ReplaceOneModel](http://mongodb.github.io/mongo-java-driver/3.6/javadoc/com/mongodb/client/model/ReplaceOneModel.html) together with [upsert mode](http://mongodb.github.io/mongo-java-driver/3.6/javadoc/com/mongodb/client/model/UpdateOptions.html) whenever inserts or updates are handled. If the connector is configured to process deletes when _null_ values of sink records are discovered then it uses a [DeleteOneModel](http://mongodb.github.io/mongo-java-driver/3.6/javadoc/com/mongodb/client/model/DeleteOneModel.html) respectively.

Data is written using acknowledged writes and the configured write concern level of the connection as specified in the connection URI. If the bulk write fails (totally or partially) errors are logged and a simple retry logic is in place. More robust/sophisticated failure mode handling has yet to be implemented.
 
### Sink Connector Properties 

At the moment the following settings can be configured by means of the *connector.properties* file. For a config file containing default settings see [this example](https://github.com/hpgrahsl/kafka-connect-mongodb/blob/master/config/MongoDbSinkConnector.properties).

| Name                                | Description                                                                            | Type    | Default                                                               | Valid Values                 | Importance |
|-------------------------------------|----------------------------------------------------------------------------------------|---------|-----------------------------------------------------------------------|------------------------------|------------|
| mongodb.collection                  | single sink collection name to write to                                                | string  | kafkatopic                                                            |                              | high       |
| mongodb.connection.uri              | the mongodb connection URI as supported by the official drivers                        | string  | mongodb://localhost:27017/kafkaconnect?w=1&journal=true               |                              | high       |
| mongodb.document.id.strategy        | class name of strategy to use for generating a unique document id (_id)                | string  | at.grahsl.kafka.connect.mongodb.processor.id.strategy.BsonOidStrategy |                              | high       |
| mongodb.delete.on.null.values       | whether or not the connector tries to delete documents based on key when value is null | boolean | false                                                                 |                              | medium     |
| mongodb.max.num.retries             | how often a retry should be done on write errors                                       | int     | 3                                                                     | [0,...]                      | medium     |
| mongodb.retries.defer.timeout       | how long in ms a retry should get deferred                                             | int     | 5000                                                                  | [0,...]                      | medium     |
| mongodb.change.data.capture.handler | class name of CDC handler to use for processing                                        | string  | ""                                                                    |                              | low        |
| mongodb.document.id.strategies      | comma separated list of custom strategy classes to register for usage                  | string  | ""                                                                    |                              | low        |
| mongodb.field.renamer.mapping       | inline JSON array with objects describing field name mappings (see docs)               | string  | []                                                                    |                              | low        |
| mongodb.field.renamer.regexp        | inline JSON array with objects describing regexp settings (see docs)                   | string  | []                                                                    |                              | low        |
| mongodb.key.projection.list         | comma separated list of field names for key projection                                 | string  | ""                                                                    |                              | low        |
| mongodb.key.projection.type         | whether or not and which key projection to use                                         | string  | none                                                                  | [none, blacklist, whitelist] | low        |
| mongodb.post.processor.chain        | comma separated list of post processor classes to build the chain with                 | string  | at.grahsl.kafka.connect.mongodb.processor.DocumentIdAdder             |                              | low        |
| mongodb.value.projection.list       | comma separated list of field names for value projection                               | string  | ""                                                                    |                              | low        |
| mongodb.value.projection.type       | whether or not and which value projection to use                                       | string  | none                                                                  | [none, blacklist, whitelist] | low        |

### Running in development

```
mvn clean package
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/MongoDbSinkConnector.properties
```

### Donate
If you like this project and want to support its further development and maintanance we are happy about your [PayPal donation](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=E3P9D3REZXTJS)

### License Information
This project is licensed according to [Apache License Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

```
Copyright (c) 2018. Hans-Peter Grahsl (grahslhp@gmail.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
