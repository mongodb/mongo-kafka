package at.grahsl.kafka.connect.mongodb.cdc.debezium.mysql;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class MysqlDeleteTest {

    public static final MysqlDelete MYSQL_DELETE = new MysqlDelete();

    public static final BsonDocument FILTER_DOC_1 =
            new BsonDocument(DBCollection.ID_FIELD_NAME,
                    new BsonDocument("id",new BsonInt32(1004)));

    public static final BsonDocument FILTER_DOC_2 =
                    new BsonDocument("id",new BsonInt32(1004));


    @Test
    @DisplayName("when valid cdc event with key doc fields then correct DeleteOneModel")
    public void testValidSinkDocumentBasedOnKey() {

        BsonDocument keyDoc = new BsonDocument("id",new BsonInt32(1004));
        BsonDocument valueDoc = new BsonDocument("op",new BsonString("d"));

        WriteModel<BsonDocument> result =
                MYSQL_DELETE.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_1,writeModel.getFilter());

    }

    @Test
    @DisplayName("when valid cdc event without key doc fields but 'before' value doc then correct DeleteOneModel")
    public void testValidSinkDocumentBasedOnBeforeValueField() {

        BsonDocument keyDoc = new BsonDocument();
        BsonDocument valueDoc = new BsonDocument("op",new BsonString("d"))
                                        .append("before",new BsonDocument("id",new BsonInt32(1004)));

        WriteModel<BsonDocument> result =
                MYSQL_DELETE.perform(new SinkDocument(keyDoc,valueDoc));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_2,writeModel.getFilter());

    }

    @Test
    @DisplayName("when missing key doc then DataException")
    public void testMissingKeyDocument() {
        assertThrows(DataException.class,() ->
                MYSQL_DELETE.perform(new SinkDocument(null,new BsonDocument()))
        );
    }

    @Test
    @DisplayName("when missing value doc then DataException")
    public void testMissingValueDocument() {
        assertThrows(DataException.class,() ->
                MYSQL_DELETE.perform(new SinkDocument(new BsonDocument(),null))
        );
    }

    @Test
    @DisplayName("when key doc and value 'before' field empty then DataException")
    public void testEmptyKeyDocAndEmptyValueBeforeField() {
        assertThrows(DataException.class,() ->
                MYSQL_DELETE.perform(new SinkDocument(new BsonDocument(),
                        new BsonDocument("op",new BsonString("d")).append("before",new BsonDocument())))
        );
    }

}
