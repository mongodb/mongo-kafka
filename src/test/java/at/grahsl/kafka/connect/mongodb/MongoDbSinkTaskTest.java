package at.grahsl.kafka.connect.mongodb;

import at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms.RdbmsHandler;
import at.grahsl.kafka.connect.mongodb.processor.id.strategy.BsonOidStrategy;
import at.grahsl.kafka.connect.mongodb.processor.id.strategy.FullKeyStrategy;
import at.grahsl.kafka.connect.mongodb.writemodel.strategy.ReplaceOneDefaultStrategy;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static avro.shaded.com.google.common.collect.Lists.partition;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
public class MongoDbSinkTaskTest {

    static class TopicSettingsAndResults {

        public final String topic;
        public final String collection;
        public final int numRecords;
        public final int batchSize;

        public Schema keySchema;
        public Object key;
        public Schema valueSchema;
        public Object value;

        public List<SinkRecord> sinkRecords;
        public List<List<SinkRecord>> expectedBatching;

        public TopicSettingsAndResults(String topic, String collection, int numRecords, int batchSize) {
            this.topic = topic;
            this.collection = collection;
            this.numRecords = numRecords;
            this.batchSize = batchSize;
        }

        public void setSinkRecords(List<SinkRecord> sinkRecords) {
            this.sinkRecords = sinkRecords;
        }

        public void setExpectedBatching(List<List<SinkRecord>> expectedBatching) {
            this.expectedBatching = expectedBatching;
        }

        public void setKeySchema(Schema keySchema) {
            this.keySchema = keySchema;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public void setValueSchema(Schema valueSchema) {
            this.valueSchema = valueSchema;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }

    @Test
    @DisplayName("test create sink record batches per topic with default topic and no batching")
    void testCreateSinkRecordBatchesPerTopicWithDefaultTopicAndNoBatching() {

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();

        TopicSettingsAndResults settings = new TopicSettingsAndResults("kafkatopic","kafkatopic",50,0);
        String namespace = "kafkaconnect."+settings.collection;

        Map<String,String> props = new HashMap<>();
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF,settings.collection);
        sinkTask.start(props);

        List<SinkRecord> sinkRecords = createSinkRecordList(settings);
        Map<String, MongoDbSinkRecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(sinkRecords);

        assertEquals(1, batchesMap.size(), "wrong number of entries in batch map");
        assertNotNull(batchesMap.get(namespace), "batch map entry for "+namespace+" was null");

        assertAll("verify contents of created batches map",
                () -> assertEquals(1, batchesMap.get(namespace).getBufferedBatches().size(),
                        "wrong number of batches in map entry for "+namespace),
                () -> assertEquals(settings.numRecords, batchesMap.get(namespace).getBufferedBatches().get(0).size(),
                        "wrong number of records in single batch of map entry for "+namespace),
                () -> assertEquals(sinkRecords, batchesMap.get(namespace).getBufferedBatches().get(0),
                        "sink record list mismatch in single batch of map entry for"+namespace)
        );

    }

    @Test
    @DisplayName("test create sink record batches per topic with NO default topic and no batching")
    void testCreateSinkRecordBatchesPerTopicWithNoDefaultTopicAndNoBatching() {

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();

        TopicSettingsAndResults settings = new TopicSettingsAndResults("kafkaesque","kafkaesque",50,0);
        String namespace = "kafkaconnect."+settings.collection;

        sinkTask.start(new HashMap<>());

        List<SinkRecord> sinkRecords = createSinkRecordList(settings);
        Map<String, MongoDbSinkRecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(sinkRecords);

        assertEquals(1, batchesMap.size(), "wrong number of entries in batch map");
        assertNotNull(batchesMap.get(namespace), "batch map entry for "+namespace+" was null");

        assertAll("verify contents of created batches map",
                () -> assertEquals(1, batchesMap.get(namespace).getBufferedBatches().size(),
                        "wrong number of batches in map entry for "+namespace),
                () -> assertEquals(settings.numRecords, batchesMap.get(namespace).getBufferedBatches().get(0).size(),
                        "wrong number of records in single batch of map entry for "+namespace),
                () -> assertEquals(sinkRecords, batchesMap.get(namespace).getBufferedBatches().get(0),
                        "sink record list mismatch in single batch of map entry for"+namespace)
        );

    }

    @Test
    @DisplayName("test create sink record batches per topic with single topic and no batching")
    void testCreateSinkRecordBatchesPerTopicWithSingleTopicAndNoBatching() {

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();

        TopicSettingsAndResults settings = new TopicSettingsAndResults("foo-topic","foo-collection",100,0);
        String namespace = "kafkaconnect."+settings.collection;

        Map<String,String> props = new HashMap<>();
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF,settings.collection);
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF+"."+settings.topic,settings.collection);
        props.put(MongoDbSinkConnectorConfig.MONGODB_MAX_BATCH_SIZE+"."+settings.collection,String.valueOf(settings.batchSize));
        sinkTask.start(props);

        List<SinkRecord> sinkRecords = createSinkRecordList(settings);
        Map<String, MongoDbSinkRecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(sinkRecords);

        assertEquals(1, batchesMap.size(), "wrong number of entries in batch map");
        assertNotNull(batchesMap.get(namespace), "batch map entry for "+namespace+" was null");

        assertAll("verify contents of created batches map",
                () -> assertEquals(1, batchesMap.get(namespace).getBufferedBatches().size(),
                        "wrong number of batches in map entry for "+namespace),
                () -> assertEquals(settings.numRecords, batchesMap.get(namespace).getBufferedBatches().get(0).size(),
                        "wrong number of records in single batch of map entry for "+namespace),
                () -> assertEquals(sinkRecords, batchesMap.get(namespace).getBufferedBatches().get(0),
                        "sink record list mismatch in single batch of map entry for"+namespace)
        );

    }

    @TestFactory
    @DisplayName("test create sink record batches per topic with multiple topics and different batch sizes")
    Collection<DynamicTest> testCreateSinkRecordBatchesPerTopicWithMultipleTopicAndDifferentBatchSizes() {

        List<DynamicTest> tests = new ArrayList<>();

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();

        String database = "kafkaconnect";

        List<TopicSettingsAndResults> settings = Arrays.asList(
            new TopicSettingsAndResults("foo-topic","foo-collection",10,2),
            new TopicSettingsAndResults("blah-topic","blah-collection",25,7),
            new TopicSettingsAndResults("xyz-topic","xyz-collection",50,11)
        );

        Map<String,String> props = new HashMap<>();
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF,
                settings.stream()
                        .map(ts -> ts.collection)
                        .collect(Collectors.joining(MongoDbSinkConnectorConfig.FIELD_LIST_SPLIT_CHAR)));

        settings.forEach(ts -> {
            props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF+"."+ts.topic,ts.collection);
            props.put(MongoDbSinkConnectorConfig.MONGODB_MAX_BATCH_SIZE+"."+ts.collection,String.valueOf(ts.batchSize));
        });

        List<SinkRecord> allRecords = new ArrayList<>();

        settings.forEach(ts -> {
                List<SinkRecord> sinkRecords = createSinkRecordList(ts);
                allRecords.addAll(sinkRecords);
                ts.setSinkRecords(sinkRecords);
                ts.setExpectedBatching(partition(sinkRecords, ts.batchSize));
            }
        );

        sinkTask.start(props);

        Map<String, MongoDbSinkRecordBatches> batchesMap =
                sinkTask.createSinkRecordBatchesPerTopic(allRecords);

        assertEquals(settings.size(), batchesMap.size(), "wrong number of entries in batch map");

        settings.forEach(ts -> {
                String namespace = database+"."+ts.collection;
                tests.add(dynamicTest("verify contents of created batches map for " + namespace , () -> {
                    MongoDbSinkRecordBatches batches = batchesMap.get(namespace);
                    assertNotNull(batches,"batches was null");
                    assertEquals(ts.expectedBatching.size(), batches.getBufferedBatches().size(),
                            "wrong number of batches for map entry "+namespace);
                    assertAll("asserting created batches for equality"+namespace,
                            Stream.iterate(0, b->b+1)
                                    .limit(batches.getBufferedBatches().size())
                                    .map(b -> (Executable) () ->
                                            assertEquals(ts.expectedBatching.get(b),
                                                    batches.getBufferedBatches().get(b),
                                                    "records mismatch in batch "+b+" for map entry "+namespace) )
                                    .collect(Collectors.toList())
                    );
                }));
            });

        return tests;
    }

    @Test
    @DisplayName("test with default config and no sink records")
    void testBuildWriteModelDefaultConfigSinkRecordsAbsent() {

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();
        sinkTask.start(new HashMap<>());

        List<? extends WriteModel> writeModelList =
                sinkTask.buildWriteModel(new ArrayList<>(),"kafkatopic");

        assertNotNull(writeModelList, "WriteModel list was null");
        assertEquals(Collections.emptyList(),writeModelList, "WriteModel list mismatch");

    }

    @Test
    @DisplayName("test ReplaceOneDefaultStragey with custom config and sink records having values")
    void testBuildReplaceOneModelsCustomConfigSinkRecordsWithValuePresent() {

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF,BsonOidStrategy.class.getName());
        props.put(MongoDbSinkConnectorConfig.MONGODB_WRITEMODEL_STRATEGY,ReplaceOneDefaultStrategy.class.getName());
        sinkTask.start(props);

        TopicSettingsAndResults settings = new TopicSettingsAndResults("kafkatopic","kafkatopic",10,0);

        Schema vs = SchemaBuilder.struct()
                .field("myValueField", Schema.INT32_SCHEMA);
        settings.setValueSchema(vs);
        settings.setValue(new Struct(vs)
                .put("myValueField", 42));

        List<SinkRecord> sinkRecordList = createSinkRecordList(settings);

        List<? extends WriteModel> writeModelList =
                sinkTask.buildWriteModel(sinkRecordList,"kafkatopic");

        assertNotNull(writeModelList, "WriteModel list was null");

        assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                writeModelList.stream().map(wm ->
                        () -> assertAll("assertions for single WriteModel",
                                () -> assertTrue(wm instanceof ReplaceOneModel),
                                () -> {
                                    ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>)wm;
                                    assertTrue(rom.getOptions().isUpsert(), "replacement expected to be done in upsert mode");
                                    BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class,null);
                                    assertEquals(1,filter.size(),"filter document should only contain "+DBCollection.ID_FIELD_NAME);
                                    assertTrue(filter.get(DBCollection.ID_FIELD_NAME).isObjectId(),"filter document _id was not of type ObjectId");
                                    assertTrue(rom.getReplacement().get(DBCollection.ID_FIELD_NAME).isObjectId(),"replacement document _id was not of type ObjectId");
                                    assertEquals(42,rom.getReplacement().getInt32("myValueField").getValue(),"myValueField's value mismatch");
                                }
                    )
                )
        );

    }

    @Test
    @DisplayName("test ReplaceOneDefaultStrategy with custom config and sink records having keys & values")
    void testBuildReplaceOneModelsCustomConfigSinkRecordsWithKeyAndValuePresent() {

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF,FullKeyStrategy.class.getName());
        props.put(MongoDbSinkConnectorConfig.MONGODB_WRITEMODEL_STRATEGY,ReplaceOneDefaultStrategy.class.getName());
        props.put("topics","blah");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF,"blah-collection");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF+"."+"blah","blah-collection");
        sinkTask.start(props);

        TopicSettingsAndResults settings = new TopicSettingsAndResults("blah","blah-collection",10,0);

        Schema ks = SchemaBuilder.struct()
                .field("myKeyField", Schema.STRING_SCHEMA);
        settings.setKeySchema(ks);
        settings.setKey(new Struct(ks)
                .put("myKeyField", "ABCD-1234"));
        Schema vs = SchemaBuilder.struct()
                .field("myValueField", Schema.INT32_SCHEMA);
        settings.setValueSchema(vs);
        settings.setValue(new Struct(vs)
                .put("myValueField", 23));

        List<SinkRecord> sinkRecordList = createSinkRecordList(settings);

        List<? extends WriteModel> writeModelList =
                sinkTask.buildWriteModel(sinkRecordList,"blah-collection");

        assertNotNull(writeModelList, "WriteModel list was null");

        assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                writeModelList.stream().map(wm ->
                        () -> assertAll("assertions for single WriteModel",
                                () -> assertTrue(wm instanceof ReplaceOneModel),
                                () -> {
                                    ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>)wm;
                                    assertTrue(rom.getOptions().isUpsert(), "replacement expected to be done in upsert mode");
                                    BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class,null);
                                    assertEquals(new BsonDocument("_id",
                                                    new BsonDocument("myKeyField",new BsonString("ABCD-1234"))),
                                                        filter);
                                    assertEquals(new BsonDocument("_id",
                                                    new BsonDocument("myKeyField",new BsonString("ABCD-1234")))
                                                        .append("myValueField",new BsonInt32(23)),
                                                rom.getReplacement());
                                }
                        )
                )
        );

    }

    @Test
    @DisplayName("test DeleteOneDefaultStrategy with custom config and sink records with keys and null values")
    void testBuildDeleteOneModelsCustomConfigSinkRecordsWithKeyAndNullValuePresent() {

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF,FullKeyStrategy.class.getName());
        props.put(MongoDbSinkConnectorConfig.MONGODB_WRITEMODEL_STRATEGY,ReplaceOneDefaultStrategy.class.getName());
        props.put(MongoDbSinkConnectorConfig.MONGODB_DELETE_ON_NULL_VALUES,"true");
        props.put("topics","foo");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF,"foo-collection");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF+"."+"foo","foo-collection");
        sinkTask.start(props);

        TopicSettingsAndResults settings = new TopicSettingsAndResults("foo","foo-collection",10,0);

        Schema ks = SchemaBuilder.struct()
                .field("myKeyField", Schema.STRING_SCHEMA);
        settings.setKeySchema(ks);
        settings.setKey(new Struct(ks)
                .put("myKeyField", "ABCD-1234"));

        List<SinkRecord> sinkRecordList = createSinkRecordList(settings);

        List<? extends WriteModel> writeModelList =
                sinkTask.buildWriteModel(sinkRecordList,"blah-collection");

        assertNotNull(writeModelList, "WriteModel list was null");

        assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                writeModelList.stream().map(wm ->
                        () -> assertAll("assertions for single WriteModel",
                                () -> assertTrue(wm instanceof DeleteOneModel),
                                () -> {
                                    DeleteOneModel<BsonDocument> dom = (DeleteOneModel<BsonDocument>)wm;
                                    BsonDocument filter = dom.getFilter().toBsonDocument(BsonDocument.class,null);
                                    assertEquals(new BsonDocument("_id",
                                                    new BsonDocument("myKeyField",new BsonString("ABCD-1234"))),
                                            filter);
                                }
                        )
                )
        );

    }

    @Test
    @DisplayName("test build WriteModelCDC for Rdbms Insert")
    void testBuildWriteModelCdcForRdbmsInsert() {

        Schema keySchema = getRdbmsKeySchemaSample();
        Schema valueSchema = getRdbmsValueSchemaSample();
        List<SinkRecord> sinkRecords = IntStream.iterate(1234,i -> i+1).limit(5)
                .mapToObj(i -> new SinkRecord("test-topic",0,
                        keySchema,new Struct(keySchema)
                                    .put("id",i),
                        valueSchema,new Struct(valueSchema)
                                    .put("op","c")
                                    .put("before", null)
                                    .put("after",
                                            new Struct(valueSchema.field("after").schema())
                                                    .put("id",i)
                                                    .put("first_name","Alice_"+i)
                                                    .put("last_name","van Wonderland")
                                                    .put("email","alice_"+i+"@wonder.land"))
                                    //.put("source",...) //NOTE: SKIPPED SINCE NOT USED AT ALL SO FAR
                        ,i - 1234
                    ))
                .collect(Collectors.toList());

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put("topics","dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF,"dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF
                        +"."+"dbserver1.catalogA.tableB","dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_CHANGE_DATA_CAPTURE_HANDLER
                        +"."+"dbserver1.catalogA.tableB",RdbmsHandler.class.getName());
        sinkTask.start(props);

        List<? extends WriteModel> writeModels =
                sinkTask.buildWriteModelCDC(sinkRecords,"dbserver1.catalogA.tableB");

        assertNotNull(writeModels, "WriteModel list was null");

        assertFalse(writeModels.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                IntStream.iterate(1234,i -> i+1).limit(5).mapToObj(
                        i -> () -> {
                            int index = i-1234;
                            WriteModel wm = writeModels.get(index);
                            assertNotNull(wm, "WriteModel at index "+index+" must not be null");
                            assertTrue(wm instanceof ReplaceOneModel);
                            ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>)wm;
                            assertTrue(rom.getOptions().isUpsert(), "replacement expected to be done in upsert mode");
                            BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class,null);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id",new BsonInt32(i))),
                                    filter);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id",new BsonInt32(i)))
                                                .append("first_name",new BsonString("Alice_"+i))
                                                .append("last_name",new BsonString("van Wonderland"))
                                                .append("email",new BsonString("alice_"+i+"@wonder.land")),
                                    rom.getReplacement());
                        }
                )
        );

    }

    @Test
    @DisplayName("test build WriteModelCDC for Rdbms Update")
    void testBuildWriteModelCdcForRdbmsUpdate() {

        Schema keySchema = getRdbmsKeySchemaSample();
        Schema valueSchema = getRdbmsValueSchemaSample();
        List<SinkRecord> sinkRecords = IntStream.iterate(1234,i -> i+1).limit(5)
                .mapToObj(i -> new SinkRecord("test-topic",0,
                        keySchema,new Struct(keySchema)
                        .put("id",i),
                        valueSchema,new Struct(valueSchema)
                        .put("op","u")
                        .put("before", new Struct(valueSchema.field("before").schema())
                                        .put("id",i)
                                        .put("first_name","Alice_"+i)
                                        .put("last_name","van Wonderland")
                                        .put("email","alice_"+i+"@wonder.land"))
                        .put("after", new Struct(valueSchema.field("after").schema())
                                .put("id",i)
                                .put("first_name","Alice"+i)
                                .put("last_name","in Wonderland")
                                .put("email","alice"+i+"@wonder.land"))
                        //.put("source",...) //NOTE: SKIPPED SINCE NOT USED AT ALL SO FAR
                        ,i - 1234
                ))
                .collect(Collectors.toList());

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put("topics","dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF,"dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF
                +"."+"dbserver1.catalogA.tableB","dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_CHANGE_DATA_CAPTURE_HANDLER
                +"."+"dbserver1.catalogA.tableB",RdbmsHandler.class.getName());
        sinkTask.start(props);

        List<? extends WriteModel> writeModels =
                sinkTask.buildWriteModelCDC(sinkRecords,"dbserver1.catalogA.tableB");

        assertNotNull(writeModels, "WriteModel list was null");

        assertFalse(writeModels.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                IntStream.iterate(1234,i -> i+1).limit(5).mapToObj(
                        i -> () -> {
                            int index = i-1234;
                            WriteModel wm = writeModels.get(index);
                            assertNotNull(wm, "WriteModel at index "+index+" must not be null");
                            assertTrue(wm instanceof ReplaceOneModel);
                            ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>)wm;
                            assertTrue(rom.getOptions().isUpsert(), "replacement expected to be done in upsert mode");
                            BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class,null);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id",new BsonInt32(i))),
                                    filter);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id",new BsonInt32(i)))
                                            .append("first_name",new BsonString("Alice"+i))
                                            .append("last_name",new BsonString("in Wonderland"))
                                            .append("email",new BsonString("alice"+i+"@wonder.land")),
                                    rom.getReplacement());
                        }
                )
        );

    }

    @Test
    @DisplayName("test build WriteModelCDC for Rdbms Delete")
    void testBuildWriteModelCdcForRdbmsDelete() {

        Schema keySchema = getRdbmsKeySchemaSample();
        Schema valueSchema = getRdbmsValueSchemaSample();
        List<SinkRecord> sinkRecords = IntStream.iterate(1234,i -> i+1).limit(5)
                .mapToObj(i -> new SinkRecord("test-topic",0,
                        keySchema,new Struct(keySchema)
                        .put("id",i),
                        valueSchema,new Struct(valueSchema)
                        .put("op","d")
                        .put("before", new Struct(valueSchema.field("before").schema())
                                .put("id",i)
                                .put("first_name","Alice"+i)
                                .put("last_name","in Wonderland")
                                .put("email","alice"+i+"@wonder.land"))
                        .put("after", null)
                        //.put("source",...) //NOTE: SKIPPED SINCE NOT USED AT ALL SO FAR
                        ,i - 1234
                ))
                .collect(Collectors.toList());

        MongoDbSinkTask sinkTask = new MongoDbSinkTask();
        Map<String,String> props = new HashMap<>();
        props.put("topics","dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF,"dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF
                +"."+"dbserver1.catalogA.tableB","dbserver1.catalogA.tableB");
        props.put(MongoDbSinkConnectorConfig.MONGODB_CHANGE_DATA_CAPTURE_HANDLER
                +"."+"dbserver1.catalogA.tableB",RdbmsHandler.class.getName());
        sinkTask.start(props);

        List<? extends WriteModel> writeModels =
                sinkTask.buildWriteModelCDC(sinkRecords,"dbserver1.catalogA.tableB");

        assertNotNull(writeModels, "WriteModel list was null");

        assertFalse(writeModels.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                IntStream.iterate(1234,i -> i+1).limit(5).mapToObj(
                        i -> () -> {
                            int index = i-1234;
                            WriteModel wm = writeModels.get(index);
                            assertNotNull(wm, "WriteModel at index "+index+" must not be null");
                            assertTrue(wm instanceof DeleteOneModel);
                            DeleteOneModel<BsonDocument> rom = (DeleteOneModel<BsonDocument>)wm;
                            BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class,null);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id",new BsonInt32(i))),
                                    filter);
                        }
                )
        );

    }

    private static Schema getRdbmsKeySchemaSample() {
        return SchemaBuilder.struct()
                .name("dbserver1.catalogA.tableB.Key")
                .field("id", Schema.INT32_SCHEMA)
                .build();
    }

    private static Schema getRdbmsValueSchemaSample() {
        return SchemaBuilder.struct()
                .name("dbserver1.catalogA.tableB.Envelope")
                .field("op",Schema.STRING_SCHEMA)
                .field("before",
                        SchemaBuilder.struct().name("dbserver1.catalogA.tableB.Value").optional()
                                .field("id", Schema.INT32_SCHEMA)
                                .field("first_name", Schema.STRING_SCHEMA)
                                .field("last_name", Schema.STRING_SCHEMA)
                                .field("email", Schema.STRING_SCHEMA))
                .field("after",
                        SchemaBuilder.struct().name("dbserver1.catalogA.tableB.Value").optional()
                                .field("id", Schema.INT32_SCHEMA)
                                .field("first_name", Schema.STRING_SCHEMA)
                                .field("last_name", Schema.STRING_SCHEMA)
                                .field("email", Schema.STRING_SCHEMA))
                //.field("source",...) //NOTE: SKIPPED SINCE NOT USED AT ALL SO FAR
                ;
    }

    private static List<SinkRecord> createSinkRecordList(TopicSettingsAndResults settings) {
        return Stream.iterate(0,r -> r+1)
                .limit(settings.numRecords)
                .map(r -> new SinkRecord(settings.topic,0,settings.keySchema,settings.key,settings.valueSchema,settings.value, r))
                .collect(Collectors.toList());
    }

}
