package at.grahsl.kafka.connect.mongodb.cdc.debezium.mysql;

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
public class MysqlInsertTest {

    public static final MysqlInsert MYSQL_INSERT = new MysqlInsert();

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

        BsonDocument valueDoc = new BsonDocument("op",new BsonString("c"))
                .append("after",new BsonDocument("id",new BsonInt32(1004))
                        .append("first_name",new BsonString("Anne"))
                        .append("last_name",new BsonString("Kretchmar"))
                        .append("email",new BsonString("annek@noanswer.org")));

        WriteModel<BsonDocument> result =
                MYSQL_INSERT.perform(new SinkDocument(keyDoc,valueDoc));

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
                MYSQL_INSERT.perform(new SinkDocument(keyDoc,valueDoc));

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

        //NOTE: for both filterDoc and replacementDoc _id is a generated ObjectId
        //which cannot be set from outside for testing thus it is set
        //by taking it from the resulting writeModel in order to do an equals comparison
        //for all contained fields

        BsonDocument filterDoc = new BsonDocument();

        BsonDocument replacementDoc =
                        new BsonDocument("text", new BsonString("lalala"))
                        .append("number", new BsonInt32(1234))
                        .append("active", new BsonBoolean(false));

        BsonDocument keyDoc = new BsonDocument();

        BsonDocument valueDoc = new BsonDocument("op",new BsonString("c"))
                .append("after",new BsonDocument("text", new BsonString("lalala"))
                        .append("number", new BsonInt32(1234))
                        .append("active", new BsonBoolean(false)));

        WriteModel<BsonDocument> result =
                MYSQL_INSERT.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertTrue(writeModel.getReplacement().isObjectId(DBCollection.ID_FIELD_NAME),
                () -> "replacement doc must contain _id field of type ObjectID");

        replacementDoc.put(DBCollection.ID_FIELD_NAME,
                writeModel.getReplacement().get(DBCollection.ID_FIELD_NAME,new BsonObjectId()));

        assertEquals(replacementDoc,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertTrue(((BsonDocument)writeModel.getFilter()).isObjectId(DBCollection.ID_FIELD_NAME),
                () -> "filter doc must contain _id field of type ObjectID");

        filterDoc.put(DBCollection.ID_FIELD_NAME,
                ((BsonDocument)writeModel.getFilter()).get(DBCollection.ID_FIELD_NAME,new BsonObjectId()));

        assertEquals(filterDoc,writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when missing key doc then DataException")
    public void testMissingKeyDocument() {
        assertThrows(DataException.class,() ->
                MYSQL_INSERT.perform(new SinkDocument(null, new BsonDocument()))
        );
    }

    @Test
    @DisplayName("when missing value doc then DataException")
    public void testMissingValueDocument() {
        assertThrows(DataException.class,() ->
            MYSQL_INSERT.perform(new SinkDocument(new BsonDocument(),null))
        );
    }

    @Test
    @DisplayName("when invalid json in value doc 'after' field then DataException")
    public void testInvalidAfterField() {
        assertThrows(DataException.class,() ->
                MYSQL_INSERT.perform(
                        new SinkDocument(new BsonDocument(),
                            new BsonDocument("op",new BsonString("c"))
                                .append("after",new BsonString("{NO : JSON [HERE] GO : AWAY}")))
                )
        );
    }

}
