package at.grahsl.kafka.connect.mongodb.cdc.debezium.mysql;

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
public class MysqlHandlerTest {

    public static final MysqlHandler MYSQL_HANDLER =
            new MysqlHandler(new MongoDbSinkConnectorConfig(new HashMap<>()));

    @Test
    @DisplayName("when key doc contains fields but value is empty then null due to tombstone")
    public void testTombstoneEvent1() {
        assertEquals(Optional.empty(),
                MYSQL_HANDLER.handle(new SinkDocument(
                        new BsonDocument("id",new BsonInt32(1234)), new BsonDocument())),
                "tombstone event must result in Optional.empty()"
        );
    }

    @Test
    @DisplayName("when both key doc and value value doc are empty then null due to tombstone")
    public void testTombstoneEvent2() {
        assertEquals(Optional.empty(),
                MYSQL_HANDLER.handle(new SinkDocument(new BsonDocument(), new BsonDocument())),
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
                MYSQL_HANDLER.handle(cdcEvent)
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
                MYSQL_HANDLER.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc is missing operation type then DataException")
    public void testMissingCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id",new BsonInt32(1234)),
                new BsonDocument("po",BsonNull.VALUE)
        );
        assertThrows(DataException.class, () ->
                MYSQL_HANDLER.handle(cdcEvent)
        );
    }

    @TestFactory
    @DisplayName("when valid cdc operation type then correct MySQL CdcOperation")
    public Stream<DynamicTest> testValidCdcOpertionTypes() {

        return Stream.of(
                dynamicTest("test operation " + OperationType.CREATE, () ->
                        assertTrue(MYSQL_HANDLER.getCdcOperation(
                                new BsonDocument("op", new BsonString("c")))
                                instanceof MysqlInsert)
                ),
                dynamicTest("test operation " + OperationType.READ, () ->
                        assertTrue(MYSQL_HANDLER.getCdcOperation(
                                new BsonDocument("op", new BsonString("r")))
                                instanceof MysqlInsert)
                ),
                dynamicTest("test operation " + OperationType.UPDATE, () ->
                        assertTrue(MYSQL_HANDLER.getCdcOperation(
                                new BsonDocument("op", new BsonString("u")))
                                instanceof MysqlUpdate)
                ),
                dynamicTest("test operation " + OperationType.DELETE, () ->
                        assertTrue(MYSQL_HANDLER.getCdcOperation(
                                new BsonDocument("op", new BsonString("d")))
                                instanceof MysqlDelete)
                )
        );

    }

}
