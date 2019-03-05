package at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.OperationType;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
public class RdbmsHandlerTest {

    public static final RdbmsHandler RDBMS_HANDLER_DEFAULT_MAPPING =
            new RdbmsHandler(new MongoDbSinkConnectorConfig(new HashMap<>()));

    public static final RdbmsHandler RDBMS_HANDLER_EMPTY_MAPPING =
            new RdbmsHandler(new MongoDbSinkConnectorConfig(new HashMap<>()),
                    new HashMap<>());

    @Test
    @DisplayName("verify existing default config from base class")
    public void testExistingDefaultConfig() {
        assertAll(
                () -> assertNotNull(RDBMS_HANDLER_DEFAULT_MAPPING.getConfig(),
                        () -> "default config for handler must not be null"),
                () -> assertNotNull(RDBMS_HANDLER_EMPTY_MAPPING.getConfig(),
                        () -> "default config for handler must not be null")
        );
    }

    @Test
    @DisplayName("when key doc contains fields but value is empty then null due to tombstone")
    public void testTombstoneEvent1() {
        assertEquals(Optional.empty(),
                RDBMS_HANDLER_DEFAULT_MAPPING.handle(new SinkDocument(
                        new BsonDocument("id", new BsonInt32(1234)), new BsonDocument())),
                "tombstone event must result in Optional.empty()"
        );
    }

    @Test
    @DisplayName("when both key doc and value value doc are empty then null due to tombstone")
    public void testTombstoneEvent2() {
        assertEquals(Optional.empty(),
                RDBMS_HANDLER_DEFAULT_MAPPING.handle(new SinkDocument(new BsonDocument(), new BsonDocument())),
                "tombstone event must result in Optional.empty()"
        );
    }

    @Test
    @DisplayName("when value doc contains unknown operation type then DataException")
    public void testUnkownCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id", new BsonInt32(1234)),
                new BsonDocument("op", new BsonString("x"))
        );
        assertThrows(DataException.class, () ->
                RDBMS_HANDLER_DEFAULT_MAPPING.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc contains unmapped operation type then DataException")
    public void testUnmappedCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id", new BsonInt32(1004)),
                new BsonDocument("op", new BsonString("c"))
                        .append("after", new BsonDocument("id", new BsonInt32(1004))
                                .append("foo", new BsonString("blah")))
        );
        assertThrows(DataException.class, () ->
                RDBMS_HANDLER_EMPTY_MAPPING.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc contains operation type other than string then DataException")
    public void testInvalidCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id", new BsonInt32(1234)),
                new BsonDocument("op", new BsonInt32('c'))
        );
        assertThrows(DataException.class, () ->
                RDBMS_HANDLER_DEFAULT_MAPPING.handle(cdcEvent)
        );
    }

    @Test
    @DisplayName("when value doc is missing operation type then DataException")
    public void testMissingCdcOperationType() {
        SinkDocument cdcEvent = new SinkDocument(
                new BsonDocument("id", new BsonInt32(1234)),
                new BsonDocument("po", BsonNull.VALUE)
        );
        assertThrows(DataException.class, () ->
                RDBMS_HANDLER_DEFAULT_MAPPING.handle(cdcEvent)
        );
    }

    @TestFactory
    @DisplayName("when valid CDC event then correct WriteModel")
    public Stream<DynamicTest> testValidCdcDocument() {

        return Stream.of(
                dynamicTest("test operation " + OperationType.CREATE, () -> {
                    Optional<WriteModel<BsonDocument>> result =
                            RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                                    new SinkDocument(
                                            new BsonDocument("id", new BsonInt32(1004)),
                                            new BsonDocument("op", new BsonString("c"))
                                                    .append("after", new BsonDocument("id", new BsonInt32(1004))
                                                            .append("foo", new BsonString("blah")))
                                    )
                            );
                    assertTrue(result.isPresent());
                    assertTrue(result.get() instanceof ReplaceOneModel,
                            () -> "result expected to be of type ReplaceOneModel");

                }),
                dynamicTest("test operation " + OperationType.READ, () -> {
                    Optional<WriteModel<BsonDocument>> result =
                            RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                                    new SinkDocument(
                                            new BsonDocument("id", new BsonInt32(1004)),
                                            new BsonDocument("op", new BsonString("r"))
                                                    .append("after", new BsonDocument("id", new BsonInt32(1004))
                                                            .append("foo", new BsonString("blah")))
                                    )
                            );
                    assertTrue(result.isPresent());
                    assertTrue(result.get() instanceof ReplaceOneModel,
                            () -> "result expected to be of type ReplaceOneModel");

                }),
                dynamicTest("test operation " + OperationType.UPDATE, () -> {
                    Optional<WriteModel<BsonDocument>> result =
                            RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                                    new SinkDocument(
                                            new BsonDocument("id", new BsonInt32(1004)),
                                            new BsonDocument("op", new BsonString("u"))
                                                    .append("after", new BsonDocument("id", new BsonInt32(1004))
                                                            .append("foo", new BsonString("blah")))
                                    )
                            );
                    assertTrue(result.isPresent());
                    assertTrue(result.get() instanceof ReplaceOneModel,
                            () -> "result expected to be of type ReplaceOneModel");

                }),
                dynamicTest("test operation " + OperationType.DELETE, () -> {
                    Optional<WriteModel<BsonDocument>> result =
                            RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                                    new SinkDocument(
                                            new BsonDocument("id", new BsonInt32(1004)),
                                            new BsonDocument("op", new BsonString("d"))
                                    )
                            );
                    assertTrue(result.isPresent(), () -> "write model result must be present");
                    assertTrue(result.get() instanceof DeleteOneModel,
                            () -> "result expected to be of type DeleteOneModel");

                })
        );

    }

    @TestFactory
    @DisplayName("when valid cdc operation type then correct RDBMS CdcOperation")
    public Stream<DynamicTest> testValidCdcOpertionTypes() {

        return Stream.of(
                dynamicTest("test operation " + OperationType.CREATE, () ->
                        assertTrue(RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                new BsonDocument("op", new BsonString("c")))
                                instanceof RdbmsInsert)
                ),
                dynamicTest("test operation " + OperationType.READ, () ->
                        assertTrue(RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                new BsonDocument("op", new BsonString("r")))
                                instanceof RdbmsInsert)
                ),
                dynamicTest("test operation " + OperationType.UPDATE, () ->
                        assertTrue(RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                new BsonDocument("op", new BsonString("u")))
                                instanceof RdbmsUpdate)
                ),
                dynamicTest("test operation " + OperationType.DELETE, () ->
                        assertTrue(RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                new BsonDocument("op", new BsonString("d")))
                                instanceof RdbmsDelete)
                )
        );

    }

}
