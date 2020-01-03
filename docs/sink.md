# MongoDB Kafka Connector

## MongoDB Kafka sink connector guide

An [Apache Kafka](https://kafka.apache.org/) [connect sink connector](https://kafka.apache.org/documentation/#connect) for 
[MongoDB](https://www.mongodb.com/). For more information about configuring connectors in general see the 
[official Confluent documentation](https://docs.confluent.io/current/connect/managing/configuring.html).

### MongoDB Persistence
The Kafka records are converted to Bson documents which are in turn inserted into the corresponding MongoDB target collection. 
According to the chosen write model strategy either a; 
[ReplaceOneModel](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/ReplaceOneModel.html) or an 
[UpdateOneModel](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/UpdateOneModel.html) will be used whenever 
inserts or updates are handled. Either model will perform an upsert if the data does not exist in the collection.

If the connector is configured to process convention-based deletes, then when _null_ values for Kakfa records are discovered a 
[DeleteOneModel](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/DeleteOneModel.html) will be used.

Data is written using the configured write concern level of the connection as specified in the 
[connection string](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/ConnectionString.html). If the bulk write fails 
(totally or partially) errors are logged and a simple retry logic is in place. 

### Sink Connector Configuration Properties 

At the moment the following settings can be configured by means of the *connector.properties* file. For an example config file see 
[MongoSinkConnector.properties](../config/MongoSinkConnector.properties).

| Name                        | Description                                                                                                                                                                                                                                                                                                                                                                                                             | Type    | Default                                                                      | Valid Values                                                                                                                                             | Importance |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| topics                      | A list of kafka topics for the sink connector.                                                                                                                                                                                                                                                                                                                                                                          | list    |                                                                              | A non-empty list                                                                                                                                         | high       |
| connection.uri              | The connection URI as supported by the official drivers. eg: ``mongodb://user@pass@locahost/``.                                                                                                                                                                                                                                                                                                                         | string  | mongodb://localhost:27017                                                    | A valid connection string                                                                                                                                | high       |
| database                    | The database for the sink to write.                                                                                                                                                                                                                                                                                                                                                                                     | string  |                                                                              | non-empty string                                                                                                                                         | high       |
| collection                  | Optional, single sink collection name to write to. If following multiple topics then this will be the default collection they are mapped to.                                                                                                                                                                                                                                                                            | string  | ""                                                                           |                                                                                                                                                          | high       |
| document.id.strategy        | The IdStrategy class name to use for generating a unique document id (_id)                                                                                                                                                                                                                                                                                                                                              | string  | com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidStrategy         | An empty string OR A string matching `(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*\.)*\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*` | high       |
| delete.on.null.values       | Whether or not the connector tries to delete documents based on key when value is null                                                                                                                                                                                                                                                                                                                                  | boolean | false                                                                        |                                                                                                                                                          | medium     |
| max.batch.size              | The maximum number of sink records to possibly batch together for processing                                                                                                                                                                                                                                                                                                                                            | int     | 0                                                                            | [0,...]                                                                                                                                                  | medium     |
| max.num.retries             | How often a retry should be done on write errors                                                                                                                                                                                                                                                                                                                                                                        | int     | 3                                                                            | [0,...]                                                                                                                                                  | medium     |
| retries.defer.timeout       | How long in ms a retry should get deferred                                                                                                                                                                                                                                                                                                                                                                              | int     | 5000                                                                         | [0,...]                                                                                                                                                  | medium     |
| change.data.capture.handler | The class name of the CDC handler to use for processing                                                                                                                                                                                                                                                                                                                                                                 | string  | ""                                                                           | An empty string OR A string matching `(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*\.)*\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*` | low        |
| field.renamer.mapping       | An inline JSON array with objects describing field name mappings. Example: `[{"oldName":"key.fieldA","newName":"field1"},{"oldName":"value.xyz","newName":"abc"}]`                                                                                                                                                                                                                                                      | string  | []                                                                           | A valid JSON array                                                                                                                                       | low        |
| field.renamer.regexp        | An inline JSON array with objects describing regexp settings. Example: `[[{"regexp":"^key\\\\..*my.*$","pattern":"my","replace":""},{"regexp":"^value\\\\..*$","pattern":"\\\\.","replace":"_"}]`                                                                                                                                                                                                                       | string  | []                                                                           | A valid JSON array                                                                                                                                       | low        |
| key.projection.list         | A comma separated list of field names for key projection                                                                                                                                                                                                                                                                                                                                                                | string  | ""                                                                           |                                                                                                                                                          | low        |
| key.projection.type         | The type of key projection to use                                                                                                                                                                                                                                                                                                                                                                                       | string  | none                                                                         | [none, blacklist, whitelist]                                                                                                                             | low        |
| post.processor.chain        | A comma separated list of post processor classes to process the data before saving to MongoDB.                                                                                                                                                                                                                                                                                                                          | list    | [com.mongodb.kafka.connect.sink.processor.DocumentIdAdder]                   | A list matching: `(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*\.)*\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*`                     | low        |
| rate.limiting.every.n       | After how many processed batches the rate limit should trigger (NO rate limiting if n=0)                                                                                                                                                                                                                                                                                                                                | int     | 0                                                                            | [0,...]                                                                                                                                                  | low        |
| rate.limiting.timeout       | How long in ms processing should wait before continue processing                                                                                                                                                                                                                                                                                                                                                        | int     | 0                                                                            | [0,...]                                                                                                                                                  | low        |
| value.projection.list       | A comma separated list of field names for value projection                                                                                                                                                                                                                                                                                                                                                              | string  | ""                                                                           |                                                                                                                                                          | low        |
| value.projection.type       | The type of value projection to use                                                                                                                                                                                                                                                                                                                                                                                     | string  | none                                                                         | [none, blacklist, whitelist]                                                                                                                             | low        |
| writemodel.strategy         | The class the handles how build the write models for the sink documents                                                                                                                                                                                                                                                                                                                                                 | string  | com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy | A string matching `(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*\.)*\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*`                    | low        |
| topic.override.%s.%s        | The overrides configuration allows for per topic customization of configuration. The customized overrides are merged with the default configuration, to create the specific configuration for a topic. For example, ``topic.override.foo.collection=bar`` will store data from the ``foo`` topic into the ``bar`` collection. Note: All configuration options apart from 'connection.uri' and 'topics' are overridable. | string  | ""                                                                           | Topic override                                                                                                                                           | low        |


#### Topic Specific Configuration Settings

The MongoDB Kafka Sink Connector, supports sinking data from multiple topics. However, as data may vary between the topics, individual 
configurations can be overriden using the `topic.override.<topicName>.<configurationName>` syntax. This allows any individual configuration
to be overridden on a per topic basis.

**Note:** The `topics` and `connection.uri` configurations are global and _cannot_ be overridden.

The following configuration fragments show how to apply different settings for the *topicA* and *topicC* topics.

```properties
# Specific processing settings for 'topicA'

topic.override.topicA.collection=collectionA
topic.override.topicA.document.id.strategy=com.mongodb.kafka.connect.sink.processor.id.strategy.UuidStrategy
topic.override.topicA.post.processor.chain=com.mongodb.kafka.connect.sink.processor.DocumentIdAdder,com.mongodb.kafka.connect.sink.processor.BlacklistValueProjector
topic.override.topicA.value.projection.type=blacklist
topic.override.topicA.value.projection.list=k2,k4
topic.override.topicA.max.batch.size=100

```

These properties result in the following actions for messages originating from the 'topicA' Kafka topic:

  - Document identity (*_id* field) will be given by a generated UUID
  - Value projection will be done using a blacklist approach in order to remove fields *k2* and *k4*
  - At most 100 documents will be written to the MongoDB collection 'collectionA' in one bulk write operation

Then there are also individual settings for topic 'topicC':

```properties
# Specific processing settings for 'topicC'

topic.override.topicC.collection=collectionC
topic.override.topicC.document.id.strategy=com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy
topic.override.topicC.post.processor.chain=com.mongodb.kafka.connect.sink.processor.WhitelistValueProjector
topic.override.topicC.value.projection.type=whitelist
topic.override.topicC.value.projection.list=k3,k5
topic.override.topicC.writemodel.strategy=com.mongodb.kafka.connect.sink.writemodel.strategy.UpdateOneTimestampsStrategy

```

These settings result in the following actions for messages originating from the 'topicC' Kafka topic:

  - Document identity (*_id* field) will be taken from the value structure of the message
  - Value projection will be done using a whitelist approach to remove only retain *k3* and *k5*
  - The chosen write model strategy will keep track of inserted and modified timestamps for each written document

#### Fallback to Defaults

All default configurations will be used, unless a specific topic override is configured.

### Supported Data Formats
The sink connector implementation is configurable in order to support:

* **AVRO** (makes use of Confluent's Kafka Schema Registry and is the recommended format)
* **JSON with Schema** (offers JSON record structure with explicit schema information)
* **JSON plain** (offers JSON record structure without any attached schema)
* **RAW JSON** (string only - JSON structure not managed by Kafka connect)

Since key and value settings can be independently configured, it is possible to work with different data formats for records' keys and 
values respectively.

_Note: Even when using RAW JSON mode i.e. with 
[StringConverter](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/connect/storage/StringConverter.html) the expected 
Strings have to be valid JSON._

See the excellent Confluent post [serializers explained](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained)
for more information about Kafka data serialization.

### Supported Sink Record Structure
Currently the connector is able to process Kafka Connect SinkRecords with
support for the following schema types [Schema.Type](https://kafka.apache.org/21/javadoc/org/apache/kafka/connect/data/Schema.Type.html):
*INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT*.

The conversion is able to generically deal with nested key or value structures based on the supported types listed above. The following 
example is based on [AVRO](https://avro.apache.org/)

```json
{"type": "record",
  "name": "Customer",
  "namespace": "com.mongodb.kafka.data.kafka.avro",
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
Besides the standard types it is possible to use [AVRO logical types](http://avro.apache.org/docs/1.8.2/spec.html#Logical+Types) in order 
to have field type support for:

  - **Decimal**
  - **Date**
  - **Time** (millis/micros)
  - **Timestamp** (millis/micros)

The following AVRO schema snippet based on exemplary logical type definitions should make this clearer:

```json
{
  "type": "record",
  "name": "MyLogicalTypesRecord",
  "namespace": "com.mongodb.kafka.data.kafka.avro",
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

Note: If you are using AVRO code generation for logical types in order to use them from a Java-based producer app you end-up with the 
following Java type mappings:

  - org.joda.time.LocalDate myDateField;
  - org.joda.time.LocalTime mytimeMillisField;
  - long myTimeMicrosField;
  - org.joda.time.DateTime myTimestampMillisField;
  - long myTimestampMicrosField;

See [this discussion](https://github.com/mongodb/mongo-kafka/issues/5) if you are interested in some more details. 
Note: AVRO 1.9.0 will support native Java 8 date time types.
Logical types can only be supported for **AVRO** and **JSON + Schema** data.


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

### Post Processing of documents

After the conversion into a Bson document, the documents undergo a **chain of post processors**. There are 4 processors to choose from:

  - **DocumentIdAdder** (mandatory): uses the configured _strategy_ (explained below) to insert an **_id field**
  - **BlacklistProjector** (optional): applicable for _key_ + _value_ structure
  - **WhitelistProjector** (optional): applicable for _key_ + _value_ structure
  - **FieldRenamer** (optional): applicable for _key_ + _value_ structure

Further post processors can be easily implemented based on the provided abstract base class 
[PostProcessor](https://github.com/mongodb/mongo-kafka/blob/master/src/main/java/com/mongodb/kafka/connect/sink/processor/PostProcessor.java)
 eg:

  - Remove fields with null values
  - Redact any fields containing sensitive information
  - etc.

The 'post.processor.chain' configuration property allows users to customize the post processor chain applied to the converted records 
before they are written to the sink. Just specify a comma separated list of fully qualified class names which provide the post processor 
implementations, either existing ones or new/customized ones, like so:

```properties
post.processor.chain=com.mongodb.kafka.connect.sink.processor.field.renaming.RenameByMapping
```

The `DocumentIdAdder` is automatically added at the very first position in the chain in case it is not present. Other than that, the chain 
can be built more or less arbitrarily.

The next section covers configuring the default post processors:

##### DocumentIdAdder
The sink connector is able to process both, the key and value parts of kafka records. After the conversion to MongoDB Bson documents, 
an *_id* field is automatically added to value documents which are finally persisted in a MongoDB collection. The *_id* itself is filled by 
the **configured document id generation strategy**, which can be one of the following:

  - A MongoDB **Bson ObjectId** (default)
  - A Java **UUID**
  - **Kafka meta-data** comprised of the string concatenation based on [topic-partition-offset] information
  - **full key** using the sink record's complete key structure
  - **provided in key** expects the sink record's key to contain an *_id* field which is used as is (error if not present or null)
  - **provided in value** expects the sink record's value to contain an *_id* field which is used as is (error if not present or null)
  - **partial key** using parts of the sink record's key structure 
  - **partial value** using parts of the sink record's value structure

_Note: the latter two of which can be configured to use the blacklist/whitelist field projection mechanisms described below._

The strategy is set by means of the following property:

```properties
document.id.strategy=com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidStrategy
```

There is a configuration property which allows to customize the applied id generation strategy. Thus, if none of the available strategies 
fits your needs, further strategies can be easily implemented based on the interface 
[IdStrategy](https://github.com/mongodb/mongo-kafka/blob/master/src/main/java/com/mongodb/kafka/connect/sink/processor/id/strategy/IdStrategy.java)

All custom strategies that should be available to the connector can be registered by specifying a list of fully qualified class names 
for the following configuration property:

```properties
document.id.strategies=...
```

**It's important to keep in mind that the chosen / implemented id strategy has direct implications on the possible delivery semantics.** 
If it's set to Bson ObjectId or UUID respectively, it can only ever guarantee at-least-once delivery of records, since new ids will result 
due to the re-processing on retries after failures. The other strategies permit exactly-once semantics if the respective fields forming 
the document *_id* are guaranteed to be unique in the first place.

##### Blacklist / Whitelist Projector (optional)

By default the current implementation converts and persists the full value structure of the sink records.
Key and/or value handling can be configured by using either a **blacklist or whitelist** approach in order to remove/keep fields
from the record's structure. By using the "." notation to access sub documents it's also supported to do 
redaction of nested fields. It is also possible to refer to fields of documents found within arrays by the same notation. 
See two concrete examples below about the behaviour of these two projection strategies.

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
[key|value].projection.type=blacklist
[key|value].projection.list=age,address.city,lut.key2,data.v
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
[key|value].projection.type=whitelist
[key|value].projection.list=age,address.city,lut.key2,data.v
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

  - Explicit null handling: the option to preserve / ignore fields with null values
  - Investigate if it makes sense to support array element access for field projections based on an index or a given value to project 
    simple/primitive type elements

##### How wildcard pattern matching works:

The configuration supports wildcard matching using a __'\*'__ character notation. A wildcard
is supported on any level in the document structure in order to include (whitelist) or
exclude (blacklist) any fieldname at the corresponding level. A part from that there is support
for __'\*\*'__ which can be used at any level to include/exclude the full sub structure
(i.e. all nested levels further down in the hierarchy).

_NOTE: A bunch of more concrete examples of field projections including wildcard pattern matching can be found in a corresponding 
[test class](https://github.com/mongodb/mongo-kafka/blob/master/src/test/java/com/mongodb/kafka/connect/sink/processor/field/projection/FieldProjectorTest.java)._

##### Whitelist examples:

The following example will include the *age* field, the *lut* field and all its immediate sub-fields (i.e. one level down): 

```properties
[key|value].projection.type=whitelist
[key|value].projection.list=age,lut.*
```


The following example will include the *active* field, the *address* field and its full sub structure (all available nested levels): 

```properties
[key|value].projection.type=whitelist
[key|value].projection.list=active,address.**
```

The final whitelist example will include all fields on the 1st and 2nd level:

```properties
[key|value].projection.type=whitelist
[key|value].projection.list=*.*
```


##### Blacklist examples:

The following example will exclude the *age* field, the *lut* field and all its immediate subfields (i.e. one level down):

```properties
[key|value].projection.type=blacklist
[key|value].projection.list=age,lut.*
```

The following example will exclude the *active* field, the *address* field and its full sub structure (all available nested levels): 

```properties
[key|value].projection.type=blacklist
[key|value].projection.list=active,address.**
```

The final blacklist example will exclude: all fields on the 1st and 2nd level:

```properties
[key|value].projection.type=blacklist
[key|value].projection.list=*.*
```


##### Field Renaming (optional)
There are two different options to rename any fields in the record, namely a simple and rigid 1:1 field name mapping or a more 
flexible approach using regular expressions. Both config options are defined by inline JSON arrays containing objects which describe the renaming.

Example 1:

```properties
field.renamer.mapping=[{"oldName":"key.fieldA","newName":"field1"},{"oldName":"value.xyz","newName":"abc"}]
```

Will:

  1. Rename field `fieldA` to `field1` in the **key document structure**
  1. Rename field `xyz` to `abc` in the **value document structure**

Example 2:

```properties
field.renamer.mapping=[{"regexp":"^key\\..*my.*$","pattern":"my","replace":""},{"regexp":"^value\\..*-.+$","pattern":"-","replace":"_"}]
```

These settings cause:

  1. **All field names of the key structure containing 'my'** to be renamed so that **'my' is removed**
  1. **All field names of the value structure containing a '-'** to be renamed by replacing **'-' with '_'**

Note: The use of the **"." character** as navigational operator in both examples. It's used in order to refer to nested fields in sub 
documents of the record structure. The prefix at the very beginning is used as a simple convention to distinguish between the _key_ and 
_value_ structure of a document.

### Custom Write Models
The default behaviour for the connector whenever documents are written to MongoDB collections is to make use of a proper 
[ReplaceOneModel](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/ReplaceOneModel.html) with 
[upsert mode](http://mongodb.github.io/mongo-java-driver/3.10/javadoc/com/mongodb/client/model/ReplaceOneModel.html) and **create the filter document based on the _id field** which results from applying the configured DocumentIdAdder in the value structure of the sink document.

However, there are other use cases which need different approaches and the **customization option for generating custom write models** 
can support these. The configuration entry (_mongodb.writemodel.strategy_) allows for such customizations. Currently, the following 
strategies are implemented:

  - **default behaviour** com.mongodb.kafka.connect.sink.writemodel.strategy.**ReplaceOneDefaultStrategy**
  - **business key** (see [use case 1](https://github.com/mongodb/mongo-kafka#use-case-1-employing-business-keys) below) 
    com.mongodb.kafka.connect.sink.writemodel.strategy.**ReplaceOneBusinessKeyStrategy**
  - **delete on null values** com.mongodb.kafka.connect.sink.writemodel.strategy.**DeleteOneDefaultStrategy** implicitly used when 
    config option _mongodb.delete.on.null.values=true_ for [convention-based deletion](https://github.com/mongodb/mongo-kafka#convention-based-deletion-on-null-values)
  - **add inserted/modified timestamps** (see [use case 2](https://github.com/mongodb/mongo-kafka#use-case-2-add-inserted-and-modified-timestamps) below) 
    com.mongodb.kafka.connect.sink.writemodel.strategy.**UpdateOneTimestampsStrategy**

_Note:_ Future versions will allow to make use of arbitrary, individual strategies that can be registered and easily used as 
_mongodb.writemodel.strategy_ configuration setting.

##### Use Case 1: Employing Business Keys
Let's say you want to re-use a unique business key found in your sink records while at the same time have _Bson ObjectIds_ created for 
the resulting MongoDB documents.

To achieve this a few simple configuration steps are necessary:

  1. Make sure to **create a unique key constraint** for the business key of your target MongoDB collection.
  1. Use the **PartialValueStrategy** as the DocumentIdAdder's strategy in order to let the connector know which fields belong to the 
     business key.
  1. Use the **ReplaceOneBusinessKeyStrategy** instead of the default behaviour.

These configuration settings then allow to have **filter documents based on the original business key but still have _Bson ObjectIds_ 
created for the _id field** during the first upsert into your target MongoDB target collection. Find below how such a setup might look like:
 
Given the following Kafka record:

```json
{ 
  "fieldA": "Anonymous", 
  "fieldB": 42,
  "active": true, 
  "values": [12.34, 23.45, 34.56, 45.67]
}
```

Together with the sink connector config:

```json
{
  "name": "mongo-sink",
  "config": {
    ...
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy",
    "value.projection.list": "fieldA,fieldB",
    "value.projection.type": "whitelist",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"
  }
}
```

This will create a MongoDB document looking like:

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

Given the following Kafka record

```json
{ 
  "_id": "ABCD-1234",
  "fieldA": "Anonymous", 
  "fieldB": 42,
  "active": true, 
  "values": [12.34, 23.45, 34.56, 45.67]
}
```

Together with the sink connector config: 

```json
{
  "name": "mdb-sink",
  "config": {
    ...
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.UpdateOneTimestampsStrategy"
  }
}
```

This will create a MongoDB document looking like:

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

If at some point in time later there was a Kafka record referring to the same _id but containing updated data:

```json
{ 
  "_id": "ABCD-1234",
  "fieldA": "anonymous", 
  "fieldB": -23,
  "active": false, 
  "values": [12.34, 23.45]
}
```

Then the existing MongoDB document will get updated together with a fresh timestamp for the **_modifiedTS** value:

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
The sink connector can also be used in a different operation mode in order to handle change data capture (CDC) events. 
Currently, the following CDC events from [Debezium](http://debezium.io/) can be processed:

* [MongoDB](http://debezium.io/docs/connectors/mongodb/) 
* [MySQL](http://debezium.io/docs/connectors/mysql/)
* [PostgreSQL](http://debezium.io/docs/connectors/postgresql/)

This effectively allows to replicate all state changes within the source databases into MongoDB collections. Debezium produces very similar 
CDC events for MySQL and PostgreSQL. The so far addressed use cases worked fine based on the same code which is why there is only one 
_RdbmsHandler_ implementation to support them both at the moment. 
 
Also note that **both serialization formats (JSON+Schema & AVRO) can be used** depending on which configuration is a better fits for your 
use case.

##### CDC Handler Configuration
The sink connector configuration offers a property called *mongodb.change.data.capture.handler* which is set to the fully qualified class 
name of the respective CDC format handler class. These classes must extend from the provided abstract class 
*[CdcHandler](https://github.com/mongodb/mongo-kafka/blob/master/src/main/java/com/mongodb/kafka/connect/sink/cdc/CdcHandler.java)*. 
As soon as this configuration property is set the connector runs in **CDC operation mode**. 

An example JSON configuration the sink connector which uses the current default implementation that is capable to process 
Debezium CDC MongoDB events. This config can be posted to the 
[Kafka connect REST endpoint](https://docs.confluent.io/current/connect/references/restapi.html) in order to run the sink connector.

```json
{
  "name": "mongo-sink-debezium-cdc",
  "config": {
    "key.converter":"io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url":"http://localhost:8081",
    "value.converter":"io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url":"http://localhost:8081",
    "connector.class": "com.mongodb.kafka.connect.sink.MongoSinkConnector",
    "topics": "myreplset.kafkaconnect.mongosrc",
    "connection.uri": "mongodb://mongodb:27017/kafkaconnect?w=1&journal=true",
    "change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler",
    "collection": "mongosink"
  }
}
```

##### Convention-based deletion on null values
There are scenarios in which there is no CDC enabled source connector in place. However, it might be required to still be able to handle 
record deletions. For these cases the sink connector can be configured to delete records in MongoDB whenever it encounters sink records 
which exhibit _null_ values. This is a simple convention that can be activated by setting the following configuration option:

```properties
delete.on.null.values=true
```

Based on this setting the sink connector tries to delete a MongoDB document from the corresponding collection based on the sink record's 
key or actually the resulting *_id* value thereof, which is generated according to the specified [DocumentIdAdder](#documentidadder).  

---
### Next

- [Installation guide](./install.md)
- MongoDB Kafka sink connector guide
- [The MongoDB Kafka source connector guide](./source.md)
- [A docker end 2 end example](../docker/README.md)
- [Changelog](./changelog.md)
