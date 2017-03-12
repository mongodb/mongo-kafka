package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.DynamicTest.*;
import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class IdStrategyTest {

    public static final int UUID_STRING_LENGTH = 36;
    public static final int BSON_OID_STRING_LENGTH = 12;
    public static final int KAFKA_META_DATA_PARTS = 3;

    @TestFactory
    @DisplayName("test different id generation strategies")
    public List<DynamicTest> testIdGenerationStrategies() {

        List<DynamicTest> idTests = new ArrayList<>();

        IdStrategy idS1 = new BsonOidStrategy();
        idTests.add(dynamicTest(BsonOidStrategy.class.getSimpleName(), () -> {
            BsonValue id = idS1.generateId(null,null);
            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonObjectId),
                    () -> assertEquals(BSON_OID_STRING_LENGTH,((BsonObjectId) id).getValue().toByteArray().length)
            );
        }));

        IdStrategy idS2 = new UuidStrategy();
        idTests.add(dynamicTest(UuidStrategy.class.getSimpleName(), () -> {
            BsonValue id = idS2.generateId(null, null);
            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(UUID_STRING_LENGTH,id.asString().getValue().length())
            );
        }));

        IdStrategy idS3 = new ProvidedStrategy(MongoDbSinkConnectorConfig.IdStrategyModes.PROVIDEDINKEY);
        idTests.add(dynamicTest(ProvidedStrategy.class.getSimpleName() + " in key", () -> {

            String idValue = "SOME_UNIQUE_ID_IN_KEY";

            SinkDocument sdWithIdInKeyDoc = new SinkDocument(
                    new BsonDocument("_id",new BsonString(idValue)),null);

            SinkDocument sdWithoutIdInKeyDoc = new SinkDocument(
                    new BsonDocument(),null);

            BsonValue id = idS3.generateId(sdWithIdInKeyDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(idValue, id.asString().getValue())
            );

            assertThrows(DataException.class,() -> idS3.generateId(sdWithoutIdInKeyDoc, null));

        }));

        IdStrategy idS4 = new ProvidedStrategy(MongoDbSinkConnectorConfig.IdStrategyModes.PROVIDEDINVALUE);
        idTests.add(dynamicTest(ProvidedStrategy.class.getSimpleName() + " in value", () -> {

            String idValue = "SOME_UNIQUE_ID_IN_VALUE";

            SinkDocument sdWithIdInValueDoc = new SinkDocument(
                   null, new BsonDocument("_id",new BsonString(idValue)));

            SinkDocument sdWithoutIdInValueDoc = new SinkDocument(
                    null,new BsonDocument());

            BsonValue id = idS4.generateId(sdWithIdInValueDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(idValue,id.asString().getValue())
            );

            assertThrows(DataException.class,() -> idS4.generateId(sdWithoutIdInValueDoc, null));

        }));

        IdStrategy idS5 = new KafkaMetaDataStrategy();
        idTests.add(dynamicTest(KafkaMetaDataStrategy.class.getSimpleName(), () -> {
            String topic = "some-topic";
            int partition = 1234;
            long offset = 9876543210L;
            SinkRecord sr = new SinkRecord(topic, partition,null,null,null,null, offset);
            BsonValue id = idS5.generateId(null, sr);
            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> {
                        String[] parts = id.asString().getValue().split(KafkaMetaDataStrategy.DELIMITER);
                        assertAll("meta data checks",
                                () -> assertEquals(KAFKA_META_DATA_PARTS,parts.length),
                                () -> assertEquals(topic, parts[0]),
                                () -> assertEquals(partition, Integer.parseInt(parts[1])),
                                () -> assertEquals(offset, Long.parseLong(parts[2]))
                        );
                    }
            );
        }));

        IdStrategy idS6 = new FullKeyStrategy();
        idTests.add(dynamicTest(FullKeyStrategy.class.getSimpleName(), () -> {

            BsonDocument keyDoc = new BsonDocument();
            keyDoc.put("myInt", new BsonInt32(123));
            keyDoc.put("myString", new BsonString("ABC"));

            SinkDocument sdWithKeyDoc = new SinkDocument(keyDoc,null);
            SinkDocument sdWithoutKeyDoc = new SinkDocument(null,null);

            BsonValue id = idS6.generateId(sdWithKeyDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonDocument),
                    () -> assertEquals(keyDoc,id.asDocument())
            );

            assertEquals(new BsonDocument(),idS6.generateId(sdWithoutKeyDoc, null));
        }));

        //TODO add 2 more for PartialKey, PartialValue Strategy

        return idTests;
    }


}
