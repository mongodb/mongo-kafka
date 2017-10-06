package at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        assertNull(
                MONGODB_HANDLER.handle(new SinkDocument(
                        new BsonDocument("id",new BsonInt32(1234)),
                            new BsonDocument())
                )
        );
    }

    @Test
    @DisplayName("when value doc contains invalid operation type then DataException")
    public void testInvalidCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id",new BsonInt32(1234)),
                        new BsonDocument("op",new BsonString("x"))
        );
        assertThrows(DataException.class, () ->
                MONGODB_HANDLER.handle(cdcEvent)
        );
    }

}
