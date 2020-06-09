/*
 * Copyright 2008-present MongoDB, Inc.
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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.sink.writemodel.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class WriteModelStrategyTest {

    private static final DeleteOneDefaultStrategy DELETE_ONE_DEFAULT_STRATEGY = new DeleteOneDefaultStrategy();
    private static final ReplaceOneDefaultStrategy REPLACE_ONE_DEFAULT_STRATEGY = new ReplaceOneDefaultStrategy();
    private static final ReplaceOneBusinessKeyStrategy REPLACE_ONE_BUSINESS_KEY_STRATEGY = new ReplaceOneBusinessKeyStrategy();
    private static final UpdateOneTimestampsStrategy UPDATE_ONE_TIMESTAMPS_STRATEGY = new UpdateOneTimestampsStrategy();

    private static final UpdateOneBusinessKeyTimestampStrategy
        UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_STRATEGY = new UpdateOneBusinessKeyTimestampStrategy();

    private static final BsonDocument FILTER_DOC_DELETE_DEFAULT = BsonDocument.parse("{_id: {id: 1234}}");
    private static final BsonDocument FILTER_DOC_REPLACE_DEFAULT = BsonDocument.parse("{_id: 1234}");
    private static final BsonDocument REPLACEMENT_DOC_DEFAULT = BsonDocument.parse("{_id: 1234, first_name: 'Grace', last_name: 'Hopper'}");
    private static final BsonDocument FILTER_DOC_REPLACE_BUSINESS_KEY = BsonDocument.parse("{first_name: 'Grace', last_name: 'Hopper'}");
    private static final BsonDocument REPLACEMENT_DOC_BUSINESS_KEY =
            BsonDocument.parse("{first_name: 'Grace', last_name: 'Hopper', active: false}");
    private static final BsonDocument FILTER_DOC_UPDATE_TIMESTAMPS = BsonDocument.parse("{_id: 1234}");

    @Test
    @DisplayName("when key document is missing for DeleteOneDefaultStrategy then DataException")
    void testDeleteOneDefaultStrategyWithMissingKeyDocument() {

        assertThrows(DataException.class, () ->
                DELETE_ONE_DEFAULT_STRATEGY.createWriteModel(
                        new SinkDocument(null, new BsonDocument())
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for DeleteOneDefaultStrategy then correct DeleteOneModel")
    void testDeleteOneDefaultStrategyWitValidSinkDocument() {

        BsonDocument keyDoc = BsonDocument.parse("{id: 1234}");

        WriteModel<BsonDocument> result =
                DELETE_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(keyDoc, null));

        assertTrue(result instanceof DeleteOneModel,
                "result expected to be of type DeleteOneModel");

        DeleteOneModel<BsonDocument> writeModel =
                (DeleteOneModel<BsonDocument>) result;

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_DELETE_DEFAULT, writeModel.getFilter());

    }

    @Test
    @DisplayName("when value document is missing for ReplaceOneDefaultStrategy then DataException")
    void testReplaceOneDefaultStrategyWithMissingValueDocument() {

        assertThrows(DataException.class, () ->
                REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for ReplaceOneDefaultStrategy then correct ReplaceOneModel")
    void testReplaceOneDefaultStrategyWithValidSinkDocument() {
        BsonDocument valueDoc = BsonDocument.parse("{_id: 1234, first_name: 'Grace', last_name: 'Hopper'}");

        WriteModel<BsonDocument> result = REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(null, valueDoc));
        assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;

        assertEquals(REPLACEMENT_DOC_DEFAULT, writeModel.getReplacement(), "replacement doc not matching what is expected");
        assertTrue(writeModel.getFilter() instanceof BsonDocument, "filter expected to be of type BsonDocument");
        assertEquals(FILTER_DOC_REPLACE_DEFAULT, writeModel.getFilter());
        assertTrue(writeModel.getReplaceOptions().isUpsert(), "replacement expected to be done in upsert mode");
    }

    @Test
    @DisplayName("when value document is missing for ReplaceOneBusinessKeyStrategy then DataException")
    void testReplaceOneBusinessKeyStrategyWithMissingValueDocument() {

        assertThrows(DataException.class, () ->
                REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), null)
                )
        );

    }

    @Test
    @DisplayName("when value document is missing an _id field for ReplaceOneBusinessKeyStrategy then DataException")
    void testReplaceOneBusinessKeyStrategyWithMissingIdFieldInValueDocument() {

        assertThrows(DataException.class, () ->
                REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(
                        new SinkDocument(new BsonDocument(), new BsonDocument())
                )
        );

    }

    @Test
    @DisplayName("when sink document is valid for ReplaceOneBusinessKeyStrategy then correct ReplaceOneModel")
    void testReplaceOneBusinessKeyStrategyWithValidSinkDocument() {
        BsonDocument valueDoc = BsonDocument.parse("{_id: {first_name: 'Grace', last_name: 'Hopper'}, "
                + "first_name: 'Grace', last_name: 'Hopper', active: false}}");

        WriteModel<BsonDocument> result = REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(new SinkDocument(null, valueDoc));
        assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;
        assertEquals(REPLACEMENT_DOC_BUSINESS_KEY, writeModel.getReplacement(), "replacement doc not matching what is expected");
        assertTrue(writeModel.getFilter() instanceof BsonDocument, "filter expected to be of type BsonDocument");

        assertEquals(FILTER_DOC_REPLACE_BUSINESS_KEY, writeModel.getFilter());
        assertTrue(writeModel.getReplaceOptions().isUpsert(), "replacement expected to be done in upsert mode");
    }

    @Test
    @DisplayName("when value document is missing for UpdateOneTimestampsStrategy then DataException")
    void testUpdateOneTimestampsStrategyWithMissingValueDocument() {
        assertThrows(DataException.class,
                () -> UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(new SinkDocument(new BsonDocument(), null)));
    }

    @Test
    @DisplayName("when sink document is valid for UpdateOneTimestampsStrategy then correct UpdateOneModel")
    void testUpdateOneTimestampsStrategyWithValidSinkDocument() {
        BsonDocument valueDoc = BsonDocument.parse("{_id: 1234, first_name: 'Grace', last_name: 'Hopper', active: false}");

        WriteModel<BsonDocument> result = UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(new SinkDocument(null, valueDoc));
        assertTrue(result instanceof UpdateOneModel, "result expected to be of type UpdateOneModel");

        UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;

        //NOTE: This test case can only check:
        // i) for both fields to be available
        // ii) having the correct BSON type (BsonDateTime)
        // iii) and be initially equal
        // The exact dateTime value is not directly testable here.
        BsonDocument updateDoc = (BsonDocument) writeModel.getUpdate();

        BsonDateTime modifiedTS = updateDoc.getDocument("$set").getDateTime(UpdateOneTimestampsStrategy.FIELD_NAME_MODIFIED_TS);
        BsonDateTime insertedTS = updateDoc.getDocument("$setOnInsert").getDateTime(UpdateOneTimestampsStrategy.FIELD_NAME_INSERTED_TS);

        assertEquals(insertedTS, modifiedTS, "modified and inserted timestamps must initially be equal");
        assertTrue(writeModel.getFilter() instanceof BsonDocument, "filter expected to be of type BsonDocument");
        assertEquals(FILTER_DOC_UPDATE_TIMESTAMPS, writeModel.getFilter());
        assertTrue(writeModel.getOptions().isUpsert(), "update expected to be done in upsert mode");
    }

    @Test
    @DisplayName("when value document is missing for UpdateOneBusinessKeyTimestampStrategy then DataException")
    void testUpdateOneBusinessKeyTimestampsStrategyWithMissingValueDocument() {
        assertThrows(DataException.class,
            () -> UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_STRATEGY.createWriteModel(new SinkDocument(new BsonDocument(), null)));
    }

    @Test
    @DisplayName("when sink document is valid for UpdateOneBusinessKeyTimestampStrategy then correct UpdateOneModel")
    void testUpdateOneBusinessKeyTimestampsStrategyWithValidSinkDocument() {
        BsonDocument valueDoc = BsonDocument.parse("{_id: 1234, first_name: 'Grace', last_name: 'Hopper', active: false}");

        WriteModel<BsonDocument> result = UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(new SinkDocument(null, valueDoc));
        assertTrue(result instanceof UpdateOneModel, "result expected to be of type UpdateOneModel");

        UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;

        //NOTE: This test case can only check:
        // i) for both fields to be available
        // ii) having the correct BSON type (BsonDateTime)
        // iii) and be initially equal
        // The exact dateTime value is not directly testable here.
        BsonDocument updateDoc = (BsonDocument) writeModel.getUpdate();

        BsonDateTime modifiedTS = updateDoc.getDocument("$set").getDateTime(UpdateOneBusinessKeyTimestampStrategy.FIELD_NAME_MODIFIED_TS);
        BsonDateTime insertedTS = updateDoc.getDocument("$setOnInsert")
            .getDateTime(UpdateOneBusinessKeyTimestampStrategy.FIELD_NAME_INSERTED_TS);

        assertEquals(insertedTS, modifiedTS, "modified and inserted timestamps must initially be equal");
        assertTrue(writeModel.getFilter() instanceof BsonDocument, "filter expected to be of type BsonDocument");
        assertEquals(FILTER_DOC_UPDATE_TIMESTAMPS, writeModel.getFilter());
        assertTrue(writeModel.getOptions().isUpsert(), "update expected to be done in upsert mode");
    }
}
