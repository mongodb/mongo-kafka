/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2017 Hans-Peter Grahsl.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
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
public class WriteModelStrategyTest {

    public static final DeleteOneDefaultStrategy DELETE_ONE_DEFAULT_STRATEGY =
            new DeleteOneDefaultStrategy();

    public static final ReplaceOneDefaultStrategy REPLACE_ONE_DEFAULT_STRATEGY =
            new ReplaceOneDefaultStrategy();

    public static final ReplaceOneBusinessKeyStrategy REPLACE_ONE_BUSINESS_KEY_STRATEGY =
            new ReplaceOneBusinessKeyStrategy();

    public static final UpdateOneTimestampsStrategy UPDATE_ONE_TIMESTAMPS_STRATEGY =
            new UpdateOneTimestampsStrategy();

    public static final BsonDocument FILTER_DOC_DELETE_DEFAULT = new BsonDocument(DBCollection.ID_FIELD_NAME,
            new BsonDocument("id", new BsonInt32(1004)));

    public static final BsonDocument FILTER_DOC_REPLACE_DEFAULT =
            new BsonDocument(DBCollection.ID_FIELD_NAME, new BsonInt32(1004));

    public static final BsonDocument REPLACEMENT_DOC_DEFAULT =
            new BsonDocument(DBCollection.ID_FIELD_NAME, new BsonInt32(1004))
                    .append("first_name", new BsonString("Anne"))
                    .append("last_name", new BsonString("Kretchmar"))
                    .append("email", new BsonString("annek@noanswer.org"));

    public static final BsonDocument FILTER_DOC_REPLACE_BUSINESS_KEY =
            new BsonDocument("first_name", new BsonString("Anne"))
                    .append("last_name", new BsonString("Kretchmar"));

    public static final BsonDocument REPLACEMENT_DOC_BUSINESS_KEY =
            new BsonDocument("first_name", new BsonString("Anne"))
                    .append("last_name", new BsonString("Kretchmar"))
                    .append("email", new BsonString("annek@noanswer.org"))
                    .append("age", new BsonInt32(23))
                    .append("active", new BsonBoolean(true));

    public static final BsonDocument FILTER_DOC_UPDATE_TIMESTAMPS =
            new BsonDocument(DBCollection.ID_FIELD_NAME, new BsonInt32(1004));

    public static final BsonDocument UPDATE_DOC_TIMESTAMPS =
            new BsonDocument(DBCollection.ID_FIELD_NAME, new BsonInt32(1004))
                    .append("first_name", new BsonString("Anne"))
                    .append("last_name", new BsonString("Kretchmar"))
                    .append("email", new BsonString("annek@noanswer.org"));

    @Test
    @DisplayName("when key document is missing for DeleteOneDefaultStrategy then DataException")
    public void testDeleteOneDefaultStrategyWithMissingKeyDocument() {

        assertThrows(DataException.class, () ->
                DELETE_ONE_DEFAULT_STRATEGY.createWriteModel(
                        new SinkDocument(null, new BsonDocument())
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for DeleteOneDefaultStrategy then correct DeleteOneModel")
    public void testDeleteOneDefaultStrategyWitValidSinkDocument() {

        BsonDocument keyDoc = new BsonDocument("id", new BsonInt32(1004));

        WriteModel<BsonDocument> result =
                DELETE_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(keyDoc, null));

        assertTrue(result instanceof DeleteOneModel,
                () -> "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_DELETE_DEFAULT, writeModel.getFilter());

    }

    @Test
    @DisplayName("when value document is missing for ReplaceOneDefaultStrategy then DataException")
    public void testReplaceOneDefaultStrategyWithMissingValueDocument() {

        assertThrows(DataException.class, () ->
                REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for ReplaceOneDefaultStrategy then correct ReplaceOneModel")
    public void testReplaceOneDefaultStrategyWithValidSinkDocument() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME, new BsonInt32(1004))
                .append("first_name", new BsonString("Anne"))
                .append("last_name", new BsonString("Kretchmar"))
                .append("email", new BsonString("annek@noanswer.org"));

        WriteModel<BsonDocument> result =
                REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(null, valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(REPLACEMENT_DOC_DEFAULT, writeModel.getReplacement(),
                () -> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_REPLACE_DEFAULT, writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when value document is missing for ReplaceOneBusinessKeyStrategy then DataException")
    public void testReplaceOneBusinessKeyStrategyWithMissingValueDocument() {

        assertThrows(DataException.class, () ->
                REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when value document is missing an _id field for ReplaceOneBusinessKeyStrategy then DataException")
    public void testReplaceOneBusinessKeyStrategyWithMissingIdFieldInValueDocument() {

        assertThrows(DataException.class, () ->
                REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), new BsonDocument())
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for ReplaceOneBusinessKeyStrategy then correct ReplaceOneModel")
    public void testReplaceOneBusinessKeyStrategyWithValidSinkDocument() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME,
                new BsonDocument("first_name", new BsonString("Anne"))
                        .append("last_name", new BsonString("Kretchmar")))
                .append("first_name", new BsonString("Anne"))
                .append("last_name", new BsonString("Kretchmar"))
                .append("email", new BsonString("annek@noanswer.org"))
                .append("age", new BsonInt32(23))
                .append("active", new BsonBoolean(true));

        WriteModel<BsonDocument> result =
                REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(new SinkDocument(null, valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                () -> "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(REPLACEMENT_DOC_BUSINESS_KEY, writeModel.getReplacement(),
                () -> "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_REPLACE_BUSINESS_KEY, writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when value document is missing for UpdateOneTimestampsStrategy then DataException")
    public void testUpdateOneTimestampsStrategyWithMissingValueDocument() {

        assertThrows(DataException.class, () ->
                UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for UpdateOneTimestampsStrategy then correct UpdateOneModel")
    public void testUpdateOneTimestampsStrategyWithValidSinkDocument() {

        BsonDocument valueDoc = new BsonDocument(DBCollection.ID_FIELD_NAME, new BsonInt32(1004))
                .append("first_name", new BsonString("Anne"))
                .append("last_name", new BsonString("Kretchmar"))
                .append("email", new BsonString("annek@noanswer.org"));

        WriteModel<BsonDocument> result =
                UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(new SinkDocument(null, valueDoc));

        assertTrue(result instanceof UpdateOneModel,
                () -> "result expected to be of type UpdateOneModel");

        UpdateOneModel<BsonDocument> writeModel =
                (UpdateOneModel<BsonDocument>) result;

        //NOTE: This test case can only check:
        // i) for both fields to be available
        // ii) having the correct BSON type (BsonDateTime)
        // iii) and be initially equal
        // The exact dateTime value is not directly testable here.
        BsonDocument updateDoc = (BsonDocument) writeModel.getUpdate();

        BsonDateTime modifiedTS = updateDoc.getDocument("$set")
                .getDateTime(UpdateOneTimestampsStrategy.FIELDNAME_MODIFIED_TS);
        BsonDateTime insertedTS = updateDoc.getDocument("$setOnInsert")
                .getDateTime(UpdateOneTimestampsStrategy.FIELDNAME_INSERTED_TS);

        assertTrue(insertedTS.equals(modifiedTS),
                () -> "modified and inserted timestamps must initially be equal");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                () -> "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_UPDATE_TIMESTAMPS, writeModel.getFilter());

        assertTrue(writeModel.getOptions().isUpsert(),
                () -> "update expected to be done in upsert mode");

    }

}
