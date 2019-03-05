package at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(JUnitPlatform.class)
public class MongoDbUpdateTest {

    public static final MongoDbUpdate MONGODB_UPDATE = new MongoDbUpdate();

    public static final BsonDocument FILTER_DOC =
            new BsonDocument(DBCollection.ID_FIELD_NAME, new BsonInt32(1004));

    public static final BsonDocument REPLACEMENT_DOC =
            new BsonDocument(DBCollection.ID_FIELD_NAME, new BsonInt32(1004))
                    .append("first_name", new BsonString("Anne"))
                    .append("last_name", new BsonString("Kretchmar"))
                    .append("email", new BsonString("annek@noanswer.org"));

    public static final BsonDocument UPDATE_DOC =
            new BsonDocument("$set", new BsonDocument("first_name", new BsonString("Anna"))
                    .append("last_name", new BsonString("Kretchmer"))
            );

    @Test
    @DisplayName("when valid doc replace cdc event then correct ReplaceOneModel")
    public void testValidSinkDocumentForReplacement() {

        BsonDocument keyDoc = new BsonDocument("id", new BsonString("1004"));

        BsonDocument valueDoc = new BsonDocument("op", new BsonString("u"))
                .append("patch", new BsonString(REPLACEMENT_DOC.toJson()));

        WriteModel<BsonDocument> result =
                MONGODB_UPDATE.perform(new SinkDocument(keyDoc, valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(REPLACEMENT_DOC, writeModel.getReplacement(),
                () -> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC, writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when valid doc change cdc event then correct UpdateOneModel")
    public void testValidSinkDocumentForUpdate() {
        BsonDocument keyDoc = new BsonDocument("id", new BsonString("1004"));

        BsonDocument valueDoc = new BsonDocument("op", new BsonString("u"))
                .append("patch", new BsonString(UPDATE_DOC.toJson()));

        WriteModel<BsonDocument> result =
                MONGODB_UPDATE.perform(new SinkDocument(keyDoc, valueDoc));

        assertTrue(result instanceof UpdateOneModel,
                () -> "result expected to be of type UpdateOneModel");

        UpdateOneModel<BsonDocument> writeModel =
                (UpdateOneModel<BsonDocument>) result;

        assertEquals(UPDATE_DOC, writeModel.getUpdate(),
                () -> "update doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC, writeModel.getFilter());

    }

    @Test
    @DisplayName("when missing value doc then DataException")
    public void testMissingValueDocument() {
        assertThrows(DataException.class, () ->
                MONGODB_UPDATE.perform(new SinkDocument(new BsonDocument(), null))
        );
    }

    @Test
    @DisplayName("when missing key doc then DataException")
    public void testMissingKeyDocument() {
        assertThrows(DataException.class, () ->
                MONGODB_UPDATE.perform(new SinkDocument(null,
                        new BsonDocument("patch", new BsonString("{}"))))
        );
    }

    @Test
    @DisplayName("when 'update' field missing in value doc then DataException")
    public void testMissingPatchFieldInValueDocument() {
        assertThrows(DataException.class, () ->
                MONGODB_UPDATE.perform(new SinkDocument(new BsonDocument("id", new BsonString("1004")),
                        new BsonDocument("nopatch", new BsonString("{}"))))
        );
    }

    @Test
    @DisplayName("when 'id' field not of type String in key doc then DataException")
    public void testIdFieldNoStringInKeyDocument() {
        assertThrows(DataException.class, () ->
                MONGODB_UPDATE.perform(new SinkDocument(new BsonDocument("id", new BsonInt32(1004)),
                        new BsonDocument("patch", new BsonString("{}"))))
        );
    }

    @Test
    @DisplayName("when 'id' field invalid JSON in key doc then DataException")
    public void testIdFieldInvalidJsonInKeyDocument() {
        assertThrows(DataException.class, () ->
                MONGODB_UPDATE.perform(new SinkDocument(new BsonDocument("id", new BsonString("{no-JSON}")),
                        new BsonDocument("patch", new BsonString("{}"))))
        );
    }

}
