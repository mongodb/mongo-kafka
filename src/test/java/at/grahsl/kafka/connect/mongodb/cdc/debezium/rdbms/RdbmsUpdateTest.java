package at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class RdbmsUpdateTest {

    public static final RdbmsUpdate RDBMS_UPDATE = new RdbmsUpdate();

    @Test
    @DisplayName("when valid cdc event with single field PK then correct ReplaceOneModel")
    public void testValidSinkDocumentSingleFieldPK() {

        BsonDocument filterDoc =
                new BsonDocument(DBCollection.ID_FIELD_NAME,
                        new BsonDocument("id",new BsonInt32(1004)));

        BsonDocument replacementDoc =
                new BsonDocument(DBCollection.ID_FIELD_NAME,
                        new BsonDocument("id",new BsonInt32(1004)))
                        .append("first_name",new BsonString("Anne"))
                        .append("last_name",new BsonString("Kretchmar"))
                        .append("email",new BsonString("annek@noanswer.org"));

        BsonDocument keyDoc = new BsonDocument("id",new BsonInt32(1004));

        BsonDocument valueDoc = new BsonDocument("op",new BsonString("u"))
                .append("after",new BsonDocument("id",new BsonInt32(1004))
                        .append("first_name",new BsonString("Anne"))
                        .append("last_name",new BsonString("Kretchmar"))
                        .append("email",new BsonString("annek@noanswer.org")));

        WriteModel<BsonDocument> result =
                RDBMS_UPDATE.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(replacementDoc,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(filterDoc,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when valid cdc event with compound PK then correct ReplaceOneModel")
    public void testValidSinkDocumentCompoundPK() {

        BsonDocument filterDoc =
                new BsonDocument(DBCollection.ID_FIELD_NAME,
                        new BsonDocument("idA",new BsonInt32(123))
                                .append("idB",new BsonString("ABC")));

        BsonDocument replacementDoc =
                new BsonDocument(DBCollection.ID_FIELD_NAME,
                        new BsonDocument("idA",new BsonInt32(123))
                                .append("idB",new BsonString("ABC")))
                        .append("number", new BsonDouble(567.89))
                        .append("active", new BsonBoolean(true));

        BsonDocument keyDoc = new BsonDocument("idA",new BsonInt32(123))
                .append("idB",new BsonString("ABC"));

        BsonDocument valueDoc = new BsonDocument("op",new BsonString("c"))
                .append("after",new BsonDocument("idA",new BsonInt32(123))
                        .append("idB",new BsonString("ABC"))
                        .append("number", new BsonDouble(567.89))
                        .append("active", new BsonBoolean(true)));

        WriteModel<BsonDocument> result =
                RDBMS_UPDATE.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(replacementDoc,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(filterDoc,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when valid cdc event without PK then correct ReplaceOneModel")
    public void testValidSinkDocumentNoPK() {

        BsonDocument filterDoc = new BsonDocument("text", new BsonString("hohoho"))
                .append("number", new BsonInt32(9876))
                .append("active", new BsonBoolean(true));

        BsonDocument replacementDoc =
                new BsonDocument("text", new BsonString("lalala"))
                        .append("number", new BsonInt32(1234))
                        .append("active", new BsonBoolean(false));

        BsonDocument keyDoc = new BsonDocument();

        BsonDocument valueDoc = new BsonDocument("op",new BsonString("u"))
                .append("before",new BsonDocument("text", new BsonString("hohoho"))
                        .append("number", new BsonInt32(9876))
                        .append("active", new BsonBoolean(true)))
                .append("after",new BsonDocument("text", new BsonString("lalala"))
                        .append("number", new BsonInt32(1234))
                        .append("active", new BsonBoolean(false)));

        WriteModel<BsonDocument> result =
                RDBMS_UPDATE.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(replacementDoc,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(filterDoc,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when missing key doc then DataException")
    public void testMissingKeyDocument() {
        assertThrows(DataException.class,() ->
                RDBMS_UPDATE.perform(new SinkDocument(null, new BsonDocument()))
        );
    }

    @Test
    @DisplayName("when missing value doc then DataException")
    public void testMissingValueDocument() {
        assertThrows(DataException.class,() ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument(),null))
        );
    }


    @Test
    @DisplayName("when 'after' field missing in value doc then DataException")
    public void testMissingAfterFieldInValueDocument() {
        assertThrows(DataException.class,() ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument("id",new BsonInt32(1004)),
                        new BsonDocument("op",new BsonString("u"))))
        );
    }

    @Test
    @DisplayName("when 'after' field empty in value doc then DataException")
    public void testEmptyAfterFieldInValueDocument() {
        assertThrows(DataException.class,() ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument("id",new BsonInt32(1004)),
                        new BsonDocument("op",new BsonString("u"))
                                .append("after",new BsonDocument())))
        );
    }

    @Test
    @DisplayName("when 'after' field null in value doc then DataException")
    public void testNullAfterFieldInValueDocument() {
        assertThrows(DataException.class,() ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument("id",new BsonInt32(1004)),
                        new BsonDocument("op",new BsonString("u"))
                                .append("after",new BsonNull())))
        );
    }

    @Test
    @DisplayName("when 'after' field no document in value doc then DataException")
    public void testNoDocumentAfterFieldInValueDocument() {
        assertThrows(DataException.class,() ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument("id",new BsonInt32(1004)),
                        new BsonDocument("op",new BsonString("u"))
                                .append("after",new BsonString("wrong type"))))
        );
    }

    @Test
    @DisplayName("when key doc and value 'before' field both empty then DataException")
    public void testEmptyKeyDocAndEmptyValueBeforeField() {
        assertThrows(DataException.class,() ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument(),
                        new BsonDocument("before",new BsonDocument())))
        );
    }

}
