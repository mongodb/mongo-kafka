package at.grahsl.kafka.connect.mongodb.cdc.debezium.mysql;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

}
