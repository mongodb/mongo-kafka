package at.grahsl.kafka.connect.mongodb.cdc.debezium.mysql;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.ReplaceOneModel;
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
public class MysqlInsertTest {

    public static final MysqlInsert MYSQL_INSERT = new MysqlInsert();

    public static final BsonDocument FILTER_DOC =
            new BsonDocument(DBCollection.ID_FIELD_NAME,
                    new BsonDocument("id",new BsonInt32(1004)));

    public static final BsonDocument REPLACEMENT_DOC =
            new BsonDocument(DBCollection.ID_FIELD_NAME,
                    new BsonDocument("id",new BsonInt32(1004)))
                    .append("first_name",new BsonString("Anne"))
                    .append("last_name",new BsonString("Kretchmar"))
                    .append("email",new BsonString("annek@noanswer.org"));

    @Test
    @DisplayName("when valid cdc event then correct ReplaceOneModel")
    public void testValidSinkDocument() {

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

        assertEquals(REPLACEMENT_DOC,writeModel.getReplacement(),
                ()-> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC,writeModel.getFilter());

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
