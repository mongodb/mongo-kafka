package at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb;

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
public class MongoDbDeleteTest {

    public static final MongoDbDelete MONGODB_DELETE = new MongoDbDelete();

    public static final BsonDocument FILTER_DOC =
            new BsonDocument(DBCollection.ID_FIELD_NAME,new BsonInt32(1004));

    @Test
    @DisplayName("when valid cdc event then correct DeleteOneModel")
    public void testValidSinkDocument() {
        BsonDocument keyDoc = new BsonDocument("id",new BsonString("1004"));

        WriteModel<BsonDocument> result =
                MONGODB_DELETE.perform(new SinkDocument(keyDoc,null));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC,writeModel.getFilter());

    }

    @Test
    @DisplayName("when missing key doc then DataException")
    public void testMissingKeyDocument() {
        assertThrows(DataException.class,() ->
                MONGODB_DELETE.perform(new SinkDocument(null,new BsonDocument()))
        );
    }

    @Test
    @DisplayName("when key doc 'id' field not of type String then DataException")
    public void testInvalidTypeIdFieldInKeyDocument() {
        BsonDocument keyDoc = new BsonDocument("id",new BsonInt32(1004));
        assertThrows(DataException.class,() ->
                MONGODB_DELETE.perform(new SinkDocument(keyDoc,new BsonDocument()))
        );
    }

    @Test
    @DisplayName("when key doc 'id' field contains invalid JSON then DataException")
    public void testInvalidJsonIdFieldInKeyDocument() {
        BsonDocument keyDoc = new BsonDocument("id",new BsonString("{,NOT:JSON,}"));
        assertThrows(DataException.class,() ->
                MONGODB_DELETE.perform(new SinkDocument(keyDoc,new BsonDocument()))
        );
    }

}
