package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.id.strategy.IdStrategy;
import com.mongodb.DBCollection;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
public class DocumentIdAdderTest {

    @Test
    @DisplayName("test _id field added by IdStrategy")
    void testAddingIdFieldByStrategy() {

        IdStrategy ids = mock(IdStrategy.class);
        when(ids.generateId(any(SinkDocument.class), ArgumentMatchers.isNull()))
                .thenReturn(new BsonObjectId(ObjectId.get()));

        DocumentIdAdder idAdder = new DocumentIdAdder(null,ids);
        SinkDocument sinkDocWithValueDoc = new SinkDocument(null,new BsonDocument());
        SinkDocument sinkDocWithoutValueDoc = new SinkDocument(null,null);

        assertAll("check for _id field when processing DocumentIdAdder",
                () -> {
                    idAdder.process(sinkDocWithValueDoc,null);
                    assertAll("_id checks",
                            () ->  assertTrue(sinkDocWithValueDoc.getValueDoc().orElseGet(() -> new BsonDocument())
                                            .keySet().contains(DBCollection.ID_FIELD_NAME),
                                    "must contain _id field in valueDoc"
                            ),
                            () -> assertTrue(sinkDocWithValueDoc.getValueDoc().orElseGet(() -> new BsonDocument())
                                   .get(DBCollection.ID_FIELD_NAME) instanceof BsonValue,
                            "_id field must be of type BsonValue")
                    );
                },
                () -> {
                    idAdder.process(sinkDocWithoutValueDoc,null);
                    assertTrue(!sinkDocWithoutValueDoc.getValueDoc().isPresent(),
                            "no _id added since valueDoc cannot not be present"
                    );
                }
        );

    }

}
