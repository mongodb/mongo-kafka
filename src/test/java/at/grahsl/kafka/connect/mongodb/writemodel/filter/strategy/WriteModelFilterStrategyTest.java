package at.grahsl.kafka.connect.mongodb.writemodel.filter.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class WriteModelFilterStrategyTest {

    public static final DeleteOneDefaultFilterStrategy DELETE_ONE_DEFAULT_FILTER_STRATEGY =
            new DeleteOneDefaultFilterStrategy();

    public static final ReplaceOneDefaultFilterStrategy REPLACE_ONE_DEFAULT_FILTER_STRATEGY =
            new ReplaceOneDefaultFilterStrategy();

    public static final ReplaceOneBusinessKeyFilterStrategy REPLACE_ONE_BUSINESS_KEY_FILTER_STRATEGY =
            new ReplaceOneBusinessKeyFilterStrategy();

    public static final BsonDocument FILTER_DOC_DELETE_DEFAULT = new BsonDocument(DBCollection.ID_FIELD_NAME,
            new BsonDocument("id",new BsonInt32(1004)));

    public static final BsonDocument FILTER_DOC_REPLACE_DEFAULT =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004));

    public static final BsonDocument REPLACEMENT_DOC_DEFAULT =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004))
                    .append("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"))
                    .append("email",new BsonString("annek@noanswer.org"));

    public static final BsonDocument FILTER_DOC_REPLACE_BUSINESS_KEY =
            new BsonDocument("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"));

    public static final BsonDocument REPLACEMENT_DOC_BUSINESS_KEY =
            new BsonDocument("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"))
                    .append("email",new BsonString("annek@noanswer.org"))
                    .append("age", new BsonInt32(23))
                    .append("active", new BsonBoolean(true));

    @Test
    @DisplayName("when key document is missing for DeleteOneDefaultFilterStrategy then DataException")
    public void testDeleteOneDefaultFilterStrategyWithMissingKeyDocument() {

        assertThrows(DataException.class,() ->
                DELETE_ONE_DEFAULT_FILTER_STRATEGY.createWriteModel(
                        new SinkDocument(null, new BsonDocument())
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for DeleteOneDefaultFilterStrategy then correct DeleteOneModel")
    public void testDeleteOneDefaultFilterStrategyWitValidSinkDocument() {

        BsonDocument keyDoc = new BsonDocument("id",new BsonInt32(1004));

        WriteModel<BsonDocument> result =
                DELETE_ONE_DEFAULT_FILTER_STRATEGY.createWriteModel(new SinkDocument(keyDoc,null));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_DELETE_DEFAULT,writeModel.getFilter());

    }

    @Test
    @DisplayName("when value document is missing for ReplaceOneDefaultFilterStrategy then DataException")
    public void testReplaceOneDefaultFilterStrategyWithMissingValueDocument() {

        assertThrows(DataException.class,() ->
                REPLACE_ONE_DEFAULT_FILTER_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for ReplaceOneDefaultFilterStrategy then correct ReplaceOneModel")
    public void testReplaceOneDefaultFilterStrategyWitValidSinkDocument() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004))
                .append("first_name",new BsonString("Anne"))
                .append("last_name",new BsonString("Kretchmar"))
                .append("email",new BsonString("annek@noanswer.org"));

        WriteModel<BsonDocument> result =
                REPLACE_ONE_DEFAULT_FILTER_STRATEGY.createWriteModel(new SinkDocument(null,valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(REPLACEMENT_DOC_DEFAULT,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_REPLACE_DEFAULT,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when value document is missing for ReplaceOneBusinessKeyFilterStrategy then DataException")
    public void testReplaceOneBusinessKeyFilterStrategyWithMissingValueDocument() {

        assertThrows(DataException.class,() ->
                REPLACE_ONE_BUSINESS_KEY_FILTER_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when value document is missing an _id field for ReplaceOneBusinessKeyFilterStrategy then DataException")
    public void testReplaceOneBusinessKeyFilterStrategyWithMissingIdFieldInValueDocument() {

        assertThrows(DataException.class,() ->
                REPLACE_ONE_BUSINESS_KEY_FILTER_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), new BsonDocument())
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for ReplaceOneBusinessKeyFilterStrategy then correct ReplaceOneModel")
    public void testReplaceOneBusinessKeyFilterStrategyWitValidSinkDocument() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME,
                new BsonDocument("first_name",new BsonString("Anne"))
                        .append("last_name",new BsonString("Kretchmar")))
                .append("first_name",new BsonString("Anne"))
                .append("last_name",new BsonString("Kretchmar"))
                .append("email",new BsonString("annek@noanswer.org"))
                .append("age", new BsonInt32(23))
                .append("active", new BsonBoolean(true));

        WriteModel<BsonDocument> result =
                REPLACE_ONE_BUSINESS_KEY_FILTER_STRATEGY.createWriteModel(new SinkDocument(null,valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(REPLACEMENT_DOC_BUSINESS_KEY,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_REPLACE_BUSINESS_KEY,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

}
