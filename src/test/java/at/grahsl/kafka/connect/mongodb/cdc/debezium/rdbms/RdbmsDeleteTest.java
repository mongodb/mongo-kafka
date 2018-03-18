package at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class RdbmsDeleteTest {

    public static final RdbmsDelete RDBMS_DELETE = new RdbmsDelete();

    @Test
    @DisplayName("when valid cdc event with single field PK then correct DeleteOneModel")
    public void testValidSinkDocumentSingleFieldPK() {

        BsonDocument filterDoc =
                new BsonDocument(DBCollection.ID_FIELD_NAME,
                        new BsonDocument("id",new BsonInt32(1004)));

        BsonDocument keyDoc = new BsonDocument("id",new BsonInt32(1004));
        BsonDocument valueDoc = new BsonDocument("op",new BsonString("d"));

        WriteModel<BsonDocument> result =
                RDBMS_DELETE.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(filterDoc,writeModel.getFilter());

    }

    @Test
    @DisplayName("when valid cdc event with compound PK then correct DeleteOneModel")
    public void testValidSinkDocumentCompoundPK() {

        BsonDocument filterDoc =
                new BsonDocument(DBCollection.ID_FIELD_NAME,
                        new BsonDocument("idA",new BsonInt32(123))
                                .append("idB",new BsonString("ABC")));

        BsonDocument keyDoc = new BsonDocument("idA",new BsonInt32(123))
                                    .append("idB",new BsonString("ABC"));
        BsonDocument valueDoc = new BsonDocument("op",new BsonString("d"));

        WriteModel<BsonDocument> result =
                RDBMS_DELETE.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(filterDoc,writeModel.getFilter());

    }

    @Test
    @DisplayName("when valid cdc event without PK then correct DeleteOneModel")
    public void testValidSinkDocumentNoPK() {

        BsonDocument filterDoc = new BsonDocument("text", new BsonString("hohoho"))
                .append("number", new BsonInt32(9876))
                .append("active", new BsonBoolean(true));

        BsonDocument keyDoc = new BsonDocument();

        BsonDocument valueDoc = new BsonDocument("op",new BsonString("c"))
                .append("before",new BsonDocument("text", new BsonString("hohoho"))
                        .append("number", new BsonInt32(9876))
                        .append("active", new BsonBoolean(true)));

        WriteModel<BsonDocument> result =
                RDBMS_DELETE.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(filterDoc,writeModel.getFilter());

    }

    @Test
    @DisplayName("when missing key doc then DataException")
    public void testMissingKeyDocument() {
        assertThrows(DataException.class,() ->
                RDBMS_DELETE.perform(new SinkDocument(null,new BsonDocument()))
        );
    }

    @Test
    @DisplayName("when missing value doc then DataException")
    public void testMissingValueDocument() {
        assertThrows(DataException.class,() ->
                RDBMS_DELETE.perform(new SinkDocument(new BsonDocument(),null))
        );
    }

    @Test
    @DisplayName("when key doc and value 'before' field both empty then DataException")
    public void testEmptyKeyDocAndEmptyValueBeforeField() {
        assertThrows(DataException.class,() ->
                RDBMS_DELETE.perform(new SinkDocument(new BsonDocument(),
                        new BsonDocument("op",new BsonString("d")).append("before",new BsonDocument())))
        );
    }

}
