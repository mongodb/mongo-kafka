package at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.OperationType;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
public class MongoDbHandlerTest {

    public static final MongoDbHandler MONGODB_HANDLER =
            new MongoDbHandler(new MongoDbSinkConnectorConfig(new HashMap<>()));

    @Test
    @DisplayName("when key document missing then DataException")
    public void testMissingKeyDocument() {
        assertThrows(DataException.class, () ->
            MONGODB_HANDLER.handle(new SinkDocument(null,null))
        );
    }

    @Test
    @DisplayName("when key doc contains 'id' field but value is empty then null due to tombstone")
    public void testTombstoneEvent() {
        assertEquals(Optional.empty(),
                MONGODB_HANDLER.handle(new SinkDocument(
                        new BsonDocument("id",new BsonInt32(1234)),
                            new BsonDocument())
                ),
                "tombstone event must result in Optional.empty()"
        );
    }

    @Test
    @DisplayName("when value doc contains unknown operation type then DataException")
    public void testUnkownCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id",new BsonInt32(1234)),
                new BsonDocument("op",new BsonString("x"))
        );
        assertThrows(DataException.class, () ->
                MONGODB_HANDLER.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc contains operation type other than string then DataException")
    public void testInvalidCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id",new BsonInt32(1234)),
                new BsonDocument("op",new BsonInt32('c'))
        );
        assertThrows(DataException.class, () ->
                MONGODB_HANDLER.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc is missing operation type then DataException")
    public void testMissingCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id",new BsonInt32(1234)),
                new BsonDocument("po", BsonNull.VALUE)
        );
        assertThrows(DataException.class, () ->
                MONGODB_HANDLER.handle(cdcEvent)
        );
    }

    @TestFactory
    @DisplayName("when valid cdc operation type then correct MongoDB CdcOperation")
    public Stream<DynamicTest> testValidCdcOpertionTypes() {

        return Stream.of(
                dynamicTest("test operation "+OperationType.CREATE, () ->
                    assertTrue(MONGODB_HANDLER.getCdcOperation(
                                    new BsonDocument("op",new BsonString("c")))
                                            instanceof MongoDbInsert)
                    ),
                dynamicTest("test operation "+OperationType.READ, () ->
                        assertTrue(MONGODB_HANDLER.getCdcOperation(
                                new BsonDocument("op",new BsonString("r")))
                                instanceof MongoDbInsert)
                ),
                dynamicTest("test operation "+OperationType.UPDATE, () ->
                        assertTrue(MONGODB_HANDLER.getCdcOperation(
                                new BsonDocument("op",new BsonString("u")))
                                instanceof MongoDbUpdate)
                ),
                dynamicTest("test operation "+OperationType.DELETE, () ->
                        assertTrue(MONGODB_HANDLER.getCdcOperation(
                                new BsonDocument("op",new BsonString("d")))
                                instanceof MongoDbDelete)
                )
        );

    }

}
