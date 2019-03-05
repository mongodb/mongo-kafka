# Kafka Connect MongoDB

[![Build Status](https://travis-ci.org/hpgrahsl/kafka-connect-mongodb.svg?branch=master)](https://travis-ci.org/hpgrahsl/kafka-connect-mongodb) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/9ce80f1868154f02ad839eb76521d582)](https://www.codacy.com/app/hpgrahsl/kafka-connect-mongodb?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=hpgrahsl/kafka-connect-mongodb&amp;utm_campaign=Badge_Grade) [![Codacy Badge](https://api.codacy.com/project/badge/Coverage/9ce80f1868154f02ad839eb76521d582)](https://www.codacy.com/app/hpgrahsl/kafka-connect-mongodb?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=hpgrahsl/kafka-connect-mongodb&amp;utm_campaign=Badge_Coverage)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=E3P9D3REZXTJS)

It's a basic [Apache Kafka](https://kafka.apache.org/) [Connect SinkConnector](https://kafka.apache.org/documentation/#connect) for [MongoDB](https://www.mongodb.com/).
The connector uses the official MongoDB [Java Driver](http://mongodb.github.io/mongo-java-driver/3.10/).
Future releases might additionally support the [asynchronous driver](http://mongodb.github.io/mongo-java-driver/3.10/driver-async/).

### Users / Testimonials

| Company |   |
|---------|---|
| [![QUDOSOFT](docs/logos/qudosoft.png)](http://www.qudosoft.de/) | "As a subsidiary of a well-established major german retailer,<br/>Qudosoft is challenged by incorporating innovative and<br/>performant concepts into existing workflows. At the core of<br/>a novel event-driven architecture, Kafka has been in<br/>experimental use since 2016, followed by Connect in 2017.<br/><br/>Since MongoDB is one of our databases of choice, we were<br/>glad to discover a production-ready sink connector for it.<br/> We use it, e.g. to persist customer contact events, making<br/>them available to applications that aren't integrated into our<br/>Kafka environment. Currently, this MongoDB sink connector<br/>runs on five workers consuming approx. 50 - 200k AVRO<br/>messages per day, which are written to a replica set." |
| [![RUNTITLE](docs/logos/runtitle.png)](https://www.runtitle.com/) | "RunTitle.com is a data-driven start-up in the Oil & Gas space.<br/>We curate mineral ownership data from millions of county<br/>records and help facilitate deals between mineral owners<br/>and buyers. We use Kafka to create an eco-system of loosely<br/>coupled, specialized applications that share information.<br/><br/>We have identified the mongodb-sink-connector to be central<br/>to our plans and were excited that it was readily enhanced to<br/>[support our particular use-case.](https://github.com/hpgrahsl/kafka-connect-mongodb#custom-write-models) The connector documentation<br/>and code are very clean and thorough. We are extremely positive<br/>about relying on OSS backed by such responsive curators." |

### Supported Sink Record Structure
Currently the connector is able to process Kafka Connect SinkRecords with
support for the following schema types [Schema.Type](https://kafka.apache.org/21/javadoc/org/apache/kafka/connect/data/Schema.Type.html):
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
Besides the standard types it is possible to use [AVRO logical types](http://avro.apache.org/docs/1.8.2/spec.html#Logical+Types) in order to have field type support for

* **Decimal**
* **Date**
* **Time** (millis/micros)
* **Timestamp** (millis/micros)

The following AVRO schema snippet based on exemplary logical type definitions should make this clearer:

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

Since key and value settings can be independently configured, it is possible to work with different data formats for records' keys and values respectively.

_NOTE: Even when using RAW JSON mode i.e. with [StringConverter](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/connect/storage/StringConverter.html) the expected Strings have to be valid and parsable JSON._

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

### Custom Write Models
The default behaviour for the connector whenever documents are written to MongoDB collections is to make use of a proper [ReplaceOneModel](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/ReplaceOneModel.html) with [upsert mode](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/ReplaceOneModel.html) and **create the filter document based on the _id field** which results from applying the configured DocumentIdAdder in the value structure of the sink document.

However, there are other use cases which need different approaches and the **customization option for generating custom write models** can support these. The configuration entry (_mongodb.writemodel.strategy_) allows for such customizations. Currently, the following strategies are implemented:

* **default behaviour** at.grahsl.kafka.connect.mongodb.writemodel.strategy.**ReplaceOneDefaultStrategy**
* **business key** (-> see [use case 1](https://github.com/hpgrahsl/kafka-connect-mongodb#use-case-1-employing-business-keys)) at.grahsl.kafka.connect.mongodb.writemodel.strategy.**ReplaceOneBusinessKeyStrategy**
* **delete on null values** at.grahsl.kafka.connect.mongodb.writemodel.strategy.**DeleteOneDefaultStrategy** implicitly used when config option _mongodb.delete.on.null.values=true_ for [convention-based deletion](https://github.com/hpgrahsl/kafka-connect-mongodb#convention-based-deletion-on-null-values)
* **add inserted/modified timestamps** (-> see [use case 2](https://github.com/hpgrahsl/kafka-connect-mongodb#use-case-2-add-inserted-and-modified-timestamps)) at.grahsl.kafka.connect.mongodb.writemodel.strategy.**UpdateOneTimestampsStrategy**

_NOTE:_ Future versions will allow to make use of arbitrary, individual strategies that can be registered and easily used as _mongodb.writemodel.strategy_ configuration setting.

##### Use Case 1: Employing Business Keys
Let's say you want to re-use a unique business key found in your sink records while at the same time have _BSON ObjectIds_ created for the resulting MongoDB documents.
To achieve this a few simple configuration steps are necessary:

1) make sure to **create a unique key constraint** for the business key of your target MongoDB collection
2) use the **PartialValueStrategy** as the DocumentIdAdder's strategy in order to let the connector know which fields belong to the business key
3) use the **ReplaceOneBusinessKeyStrategy** instead of the default behaviour

These configuration settings then allow to have **filter documents based on the original business key but still have _BSON ObjectIds_ created for the _id field** during the first upsert into your target MongoDB target collection. Find below how such a setup might look like:
 
Given the following fictional Kafka record

```json
{ 
  "fieldA": "Anonymous", 
  "fieldB": 42,
  "active": true, 
  "values": [12.34, 23.45, 34.56, 45.67]
}
```

together with the sink connector config below 

```json
{
  "name": "mdb-sink",
  "config": {
    ...
    "mongodb.document.id.strategy": "at.grahsl.kafka.connect.mongodb.processor.id.strategy.PartialValueStrategy",
    "mongodb.key.projection.list": "fieldA,fieldB",
    "mongodb.key.projection.type": "whitelist",
    "mongodb.writemodel.strategy": "at.grahsl.kafka.connect.mongodb.writemodel.strategy.ReplaceOneBusinessKeyStrategy"
  }
}
```

will eventually result in a MongoDB document looking like:

```json
{ 
  "_id": ObjectId("5abf52cc97e51aae0679d237"),
  "fieldA": "Anonymous", 
  "fieldB": 42,
  "active": true, 
  "values": [12.34, 23.45, 34.56, 45.67]
}
```

All upsert operations are done based on the unique business key which for this example is a compound one that consists of the two fields _(fieldA,fieldB)_.

##### Use Case 2: Add Inserted and Modified Timestamps
Let's say you want to attach timestamps to the resulting MongoDB documents such that you can store the point in time of the document insertion and at the same time maintain a second timestamp reflecting when a document was modified.

All that needs to be done is use the **UpdateOneTimestampsStrategy** instead of the default behaviour. What results from this is that
the custom write model will take care of attaching two timestamps to MongoDB documents:

1) **_insertedTS**: will only be set once in case the upsert operation results in a new MongoDB document being inserted into the corresponding collection
2) **_modifiedTS**: will be set each time the upsert operation
results in an existing MongoDB document being updated in the corresponding collection

Given the following fictional Kafka record

```json
{ 
  "_id": "ABCD-1234",
  "fieldA": "Anonymous", 
  "fieldB": 42,
  "active": true, 
  "values": [12.34, 23.45, 34.56, 45.67]
}
```

together with the sink connector config below 

```json
{
  "name": "mdb-sink",
  "config": {
    ...
    "mongodb.document.id.strategy": "at.grahsl.kafka.connect.mongodb.processor.id.strategy.ProvidedInValueStrategy",
    "mongodb.writemodel.strategy": "at.grahsl.kafka.connect.mongodb.writemodel.strategy.UpdateOneTimestampsStrategy"
  }
}
```

will result in a new MongoDB document looking like:

```json
{ 
  "_id": "ABCD-1234",
  "_insertedTS": ISODate("2018-07-22T09:19:000Z"),
  "_modifiedTS": ISODate("2018-07-22T09:19:000Z"),
  "fieldA": "Anonymous",
  "fieldB": 42,
  "active": true, 
  "values": [12.34, 23.45, 34.56, 45.67]
}
```

If at some point in time later there is a Kafka record referring to the same _id but containing updated data

```json
{ 
  "_id": "ABCD-1234",
  "fieldA": "anonymous", 
  "fieldB": -23,
  "active": false, 
  "values": [12.34, 23.45]
}
```

then the existing MongoDB document will get updated together with a fresh timestamp for the **_modifiedTS** value:

```json
{ 
  "_id": "ABCD-1234",
  "_insertedTS": ISODate("2018-07-22T09:19:000Z"),
  "_modifiedTS": ISODate("2018-07-31T19:09:000Z"),
  "fieldA": "anonymous",
  "fieldB": -23,
  "active": false, 
  "values": [12.34, 23.45]
}
```

### Change Data Capture Mode
The sink connector can also be used in a different operation mode in order to handle change data capture (CDC) events. Currently, the following CDC events from [Debezium](http://debezium.io/) can be processed:

* [MongoDB](http://debezium.io/docs/connectors/mongodb/) 
* [MySQL](http://debezium.io/docs/connectors/mysql/)
* [PostgreSQL](http://debezium.io/docs/connectors/postgresql/)
* **Oracle** ([incubating at Debezium Project](http://debezium.io/docs/connectors/oracle/))
* **SQL Server** ([incubating at Debezium Project](http://debezium.io/docs/connectors/sqlserver/))

This effectively allows to replicate all state changes within the source databases into MongoDB collections. Debezium produces very similar CDC events for MySQL and PostgreSQL. The so far addressed use cases worked fine based on the same code which is why there is only one _RdbmsHandler_ implementation to support them both at the moment. Compatibility with Debezium's Oracle & SQL Server CDC format will be addressed in a future release.
 
Also note that **both serialization formats (JSON+Schema & AVRO) can be used** depending on which configuration is a better fit for your use case.

##### CDC Handler Configuration
The sink connector configuration offers a property called *mongodb.change.data.capture.handler* which is set to the fully qualified class name of the respective CDC format handler class. These classes must extend from the provided abstract class *[CdcHandler](https://github.com/hpgrahsl/kafka-connect-mongodb/blob/master/src/main/java/at/grahsl/kafka/connect/mongodb/cdc/CdcHandler.java)*. As soon as this configuration property is set the connector runs in **CDC operation mode**. Find below a JSON based configuration sample for the sink connector which uses the current default implementation that is capable to process Debezium CDC MongoDB events. This config can be posted to the [Kafka connect REST endpoint](https://docs.confluent.io/current/connect/references/restapi.html) in order to run the sink connector.

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

##### Convention-based deletion on null values
There are scenarios in which there is no CDC enabled source connector in place. However, it might be required to still be able to handle record deletions. For these cases the sink connector can be configured to delete records in MongoDB whenever it encounters sink records which exhibit _null_ values. This is a simple convention that can be activated by setting the following configuration option:

```properties
mongodb.delete.on.null.values=true
```

Based on this setting the sink connector tries to delete a MongoDB document from the corresponding collection based on the sink record's key or actually the resulting *_id* value thereof, which is generated according to the specified [DocumentIdAdder](https://github.com/hpgrahsl/kafka-connect-mongodb#documentidadder-mandatory).  

### MongoDB Persistence
The sink records are converted to BSON documents which are in turn inserted into the corresponding MongoDB target collection. The implementation uses unorderd bulk writes. According to the chosen write model strategy either a [ReplaceOneModel](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/ReplaceOneModel.html) or an [UpdateOneModel](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/ReplaceOneModel.html) - both of which are run in [upsert mode](http://mongodb.github.io/mongo-java-driver/3.6/javadoc/com/mongodb/client/model/UpdateOptions.html) - is used whenever inserts or updates are handled. If the connector is configured to process convention-based deletes when _null_ values of sink records are discovered then it uses a [DeleteOneModel](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/ReplaceOneModel.html) respectively.

Data is written using acknowledged writes and the configured write concern level of the connection as specified in the connection URI. If the bulk write fails (totally or partially) errors are logged and a simple retry logic is in place. More robust/sophisticated failure mode handling has yet to be implemented.
 
### Sink Connector Configuration Properties 

At the moment the following settings can be configured by means of the *connector.properties* file. For a config file containing default settings see [this example](https://github.com/hpgrahsl/kafka-connect-mongodb/blob/master/config/MongoDbSinkConnector.properties).

| Name                                | Description                                                                                          | Type    | Default                                                                       | Valid Values                                                                                                     | Importance |
|-------------------------------------|------------------------------------------------------------------------------------------------------|---------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|------------|
| mongodb.collection                  | single sink collection name to write to                                                              | string  | ""                                                                            |                                                                                                                  | high       |
| mongodb.connection.uri              | the monogdb connection URI as supported by the offical drivers                                       | string  | mongodb://localhost:27017/kafkaconnect?w=1&journal=true                       |                                                                                                                  | high       |
| mongodb.document.id.strategy        | class name of strategy to use for generating a unique document id (_id)                              | string  | at.grahsl.kafka.connect.mongodb.processor.id.strategy.BsonOidStrategy         |                                                                                                                  | high       |
| mongodb.collections                 | names of sink collections to write to for which there can be topic-level specific properties defined | string  | ""                                                                            |                                                                                                                  | medium     |
| mongodb.delete.on.null.values       | whether or not the connector tries to delete documents based on key when value is null               | boolean | false                                                                         |                                                                                                                  | medium     |
| mongodb.max.batch.size              | maximum number of sink records to possibly batch together for processing                             | int     | 0                                                                             | [0,...]                                                                                                          | medium     |
| mongodb.max.num.retries             | how often a retry should be done on write errors                                                     | int     | 3                                                                             | [0,...]                                                                                                          | medium     |
| mongodb.retries.defer.timeout       | how long in ms a retry should get deferred                                                           | int     | 5000                                                                          | [0,...]                                                                                                          | medium     |
| mongodb.change.data.capture.handler | class name of CDC handler to use for processing                                                      | string  | ""                                                                            |                                                                                                                  | low        |
| mongodb.document.id.strategies      | comma separated list of custom strategy classes to register for usage                                | string  | ""                                                                            |                                                                                                                  | low        |
| mongodb.field.renamer.mapping       | inline JSON array with objects describing field name mappings                                        | string  | []                                                                            |                                                                                                                  | low        |
| mongodb.field.renamer.regexp        | inline JSON array with objects describing regexp settings                                            | string  | []                                                                            |                                                                                                                  | low        |
| mongodb.key.projection.list         | comma separated list of field names for key projection                                               | string  | ""                                                                            |                                                                                                                  | low        |
| mongodb.key.projection.type         | whether or not and which key projection to use                                                       | string  | none                                                                          | [none, blacklist, whitelist]                                                                                     | low        |
| mongodb.post.processor.chain        | comma separated list of post processor classes to build the chain with                               | string  | at.grahsl.kafka.connect.mongodb.processor.DocumentIdAdder                     |                                                                                                                  | low        |
| mongodb.rate.limiting.every.n       | after how many processed batches the rate limit should trigger (NO rate limiting if n=0)             | int     | 0                                                                             | [0,...]                                                                                                          | low        |
| mongodb.rate.limiting.timeout       | how long in ms processing should wait before continue processing                                     | int     | 0                                                                             | [0,...]                                                                                                          | low        |
| mongodb.value.projection.list       | comma separated list of field names for value projection                                             | string  | ""                                                                            |                                                                                                                  | low        |
| mongodb.value.projection.type       | whether or not and which value projection to use                                                     | string  | none                                                                          | [none, blacklist, whitelist]                                                                                     | low        |
| mongodb.writemodel.strategy         | how to build the write models for the sink documents                                                 | string  | at.grahsl.kafka.connect.mongodb.writemodel.strategy.ReplaceOneDefaultStrategy |                                                                                                                  | low        |

The above listed *connector.properties* are the 'original' (still valid / supported) way to configure the sink connector. The main drawback with it is that only one MongoDB collection could be used so far to sink data from either a single / multiple Kafka topic(s).

### Collection-aware Configuration Settings

In the past several sink connector instances had to be configured and run separately, one for each topic / collection which needed to have individual processing settings applied. Starting with version 1.2.0 it is possible to **configure multiple Kafka topic <-> MongoDB collection mappings.** This allows for a lot more flexibility and **supports complex data processing needs within one and the same sink connector instance.**

Essentially all relevant *connector.properties* can now be defined individually for each topic / collection.
 
##### Topic <-> Collection Mappings

The most important change in configuration options is about defining the named-relation between configured Kafka topics and MongoDB collections like so:

```properties

#Kafka topics to consume from
topics=foo-t,blah-t

#MongoDB collections to write to
mongodb.collections=foo-c,blah-c

#Named topic <-> collection mappings
mongodb.collection.foo-t=foo-c
mongodb.collection.blah-t=blah-c

```

**NOTE:** In case there is no explicit mapping between Kafka topic names and MongoDB collection names the following convention applies:

* if the configuration property for ```mongodb.collection``` is set to any non-empty string this MongoDB collection name will be taken for any Kafka topic for which there is no defined mapping
* if no _default name_ is configured with the above configuration property the connector falls back to using the original Kafka topic name as MongoDB collection name   

##### Individual Settings for each Collection

Configuration properties can then be defined specifically for any of the collections for which there is a named mapping defined. The following configuration fragments show how to apply different settings for *foo-c* and *blah-c* MongoDB sink collections.

```properties

#specific processing settings for topic 'foo-t' -> collection 'foo-c'

mongodb.document.id.strategy.foo-c=at.grahsl.kafka.connect.mongodb.processor.id.strategy.UuidStrategy
mongodb.post.processor.chain.foo-c=at.grahsl.kafka.connect.mongodb.processor.DocumentIdAdder,at.grahsl.kafka.connect.mongodb.processor.BlacklistValueProjector
mongodb.value.projection.type.foo-c=blacklist
mongodb.value.projection.list.foo-c=k2,k4 
mongodb.max.batch.size.foo-c=100

```

These properties result in the following actions for messages originating form Kafka topic 'foo-t':

* document identity (*_id* field) will be given by a generated UUID
* value projection will be done using a blacklist approach in order to remove fields *k2* and *k4*
* at most 100 documents will be written to the MongoDB collection 'foo-c' in one bulk write operation

Then there are also individual settings for collection 'blah-c':

```properties

#specific processing settings for topic 'blah-t' -> collection 'blah-c'

mongodb.document.id.strategy.blah-c=at.grahsl.kafka.connect.mongodb.processor.id.strategy.ProvidedInValueStrategy
mongodb.post.processor.chain.blah-c=at.grahsl.kafka.connect.mongodb.processor.WhitelistValueProjector
mongodb.value.projection.type.blah-c=whitelist
mongodb.value.projection.list.blah-c=k3,k5 
mongodb.writemodel.strategy.blah-c=at.grahsl.kafka.connect.mongodb.writemodel.strategy.UpdateOneTimestampsStrategy

```

These settings result in the following actions for messages originating from Kafka topic 'blah-t':

* document identity (*_id* field) will be taken from the value structure of the message
* value projection will be done using a whitelist approach to remove only retain *k3* and *k5*
* the chosen write model strategy will keep track of inserted and modified timestamps for each written document

##### Fallback to Defaults

Whenever the sink connector tries to apply collection specific settings where no such settings are in place, it automatically falls back to either:

* what was explicitly configured for the same collection-agnostic property

or

* what is implicitly defined for the same collection-agnostic property

For instance, given the following configuration fragment:

```properties

#explicitly defined fallback for document identity
mongodb.document.id.strategy=at.grahsl.kafka.connect.mongodb.processor.id.strategy.FullKeyStrategy

#collections specific overriding for document identity
mongodb.document.id.strategy.foo-c=at.grahsl.kafka.connect.mongodb.processor.id.strategy.UuidStrategy

#collections specific overriding for write model
mongodb.writemodel.strategy.blah-c=at.grahsl.kafka.connect.mongodb.writemodel.strategy.UpdateOneTimestampsStrategy

```

means that:

* **document identity would fallback to the explicitly given default which is the *FullKeyStrategy*** for all collections other than 'foo-c' for which it uses the specified *UuidStrategy*
* **write model strategy would fallback to the implicitly defined *ReplaceOneDefaultStrategy*** for all collections other than 'blah-c' for which it uses the specified *UpdateOneTimestampsStrategy*

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
Copyright (c) 2019. Hans-Peter Grahsl (grahslhp@gmail.com)

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
