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

package com.mongodb.kafka.connect.sink.cdc.attunity.rdbms.oracle;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.bson.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
class AttunityRdbmsUpdateTest {

    private static final AttunityRdbmsUpdate RDBMS_UPDATE = new AttunityRdbmsUpdate();

    @Test
    @DisplayName("when valid cdc event with single field PK then correct ReplaceOneModel")
    void testValidSinkDocumentSingleFieldPK() {

        BsonDocument filterDoc =
                new BsonDocument("_id",
                        BsonDocument.parse("{id: 1234}"));

        BsonDocument replacementDoc =
                new BsonDocument("_id",
                        BsonDocument.parse("{id: 1234}"))
                        .append("first_name", new BsonString("Anne"))
                        .append("last_name", new BsonString("Kretchmar"))
                        .append("email", new BsonString("annek@noanswer.org"));

        BsonDocument keyDoc = BsonDocument.parse("{id: 1234}");

        BsonDocument valueDoc = new BsonDocument("message", new BsonDocument("headers", new BsonDocument("operation", new BsonString("UPDATE")))
                .append("data", BsonDocument.parse("{id: 1234}")
                        .append("first_name", new BsonString("Anne"))
                        .append("last_name", new BsonString("Kretchmar"))
                        .append("email", new BsonString("annek@noanswer.org"))));

        WriteModel<BsonDocument> result =
                RDBMS_UPDATE.perform(new SinkDocument(keyDoc, valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(replacementDoc, writeModel.getReplacement(),
                "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                "filter expected to be of type BsonDocument");

        assertEquals(filterDoc, writeModel.getFilter());

        assertTrue(writeModel.getReplaceOptions().isUpsert(),
                "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when valid cdc event with compound PK then correct ReplaceOneModel")
    void testValidSinkDocumentCompoundPK() {

        BsonDocument filterDoc =
                new BsonDocument("_id",
                        new BsonDocument("idA", new BsonInt32(123))
                                .append("idB", new BsonString("ABC")));

        BsonDocument replacementDoc =
                new BsonDocument("_id",
                        new BsonDocument("idA", new BsonInt32(123))
                                .append("idB", new BsonString("ABC")))
                        .append("number", new BsonDouble(567.89))
                        .append("active", new BsonBoolean(true));

        BsonDocument keyDoc = new BsonDocument("idA", new BsonInt32(123))
                .append("idB", new BsonString("ABC"));

        BsonDocument valueDoc = new BsonDocument("message", new BsonDocument("headers", new BsonDocument("operation", new BsonString("UPDATE")))
                .append("data", new BsonDocument("idA", new BsonInt32(123))
                        .append("idB", new BsonString("ABC"))
                        .append("number", new BsonDouble(567.89))
                        .append("active", new BsonBoolean(true))));

        WriteModel<BsonDocument> result =
                RDBMS_UPDATE.perform(new SinkDocument(keyDoc, valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(replacementDoc, writeModel.getReplacement(),
                "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                "filter expected to be of type BsonDocument");

        assertEquals(filterDoc, writeModel.getFilter());

        assertTrue(writeModel.getReplaceOptions().isUpsert(),
                "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when valid cdc event without PK then correct ReplaceOneModel")
    void testValidSinkDocumentNoPK() {

        BsonDocument filterDoc = new BsonDocument("text", new BsonString("hohoho"))
                .append("number", new BsonInt32(9876))
                .append("active", new BsonBoolean(true));

        BsonDocument replacementDoc =
                new BsonDocument("text", new BsonString("lalala"))
                        .append("number", new BsonInt32(1234))
                        .append("active", new BsonBoolean(false));

        BsonDocument keyDoc = new BsonDocument();

        BsonDocument valueDoc = new BsonDocument("message", new BsonDocument("headers", new BsonDocument("operation", new BsonString("UPDATE")))
                .append("beforeData", new BsonDocument("text", new BsonString("hohoho"))
                        .append("number", new BsonInt32(9876))
                        .append("active", new BsonBoolean(true)))
                .append("data", new BsonDocument("text", new BsonString("lalala"))
                        .append("number", new BsonInt32(1234))
                        .append("active", new BsonBoolean(false))));

        WriteModel<BsonDocument> result =
                RDBMS_UPDATE.perform(new SinkDocument(keyDoc, valueDoc));

        assertTrue(result instanceof ReplaceOneModel,
                "result expected to be of type ReplaceOneModel");

        ReplaceOneModel<BsonDocument> writeModel =
                (ReplaceOneModel<BsonDocument>) result;

        assertEquals(replacementDoc, writeModel.getReplacement(),
                "replacement doc not matching what is expected");

        assertTrue(writeModel.getFilter() instanceof BsonDocument,
                "filter expected to be of type BsonDocument");

        assertEquals(filterDoc, writeModel.getFilter());

        assertTrue(writeModel.getReplaceOptions().isUpsert(),
                "replacement expected to be done in upsert mode");

    }

    @Test
    @DisplayName("when missing key doc then DataException")
    void testMissingKeyDocument() {
        assertThrows(DataException.class, () ->
                RDBMS_UPDATE.perform(new SinkDocument(null, new BsonDocument()))
        );
    }

    @Test
    @DisplayName("when missing value doc then DataException")
    void testMissingValueDocument() {
        assertThrows(DataException.class, () ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument(), null))
        );
    }


    @Test
    @DisplayName("when 'after' field missing in value doc then DataException")
    void testMissingAfterFieldInValueDocument() {
        assertThrows(DataException.class, () ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument("op", new BsonString("UPDATE")),
                        BsonDocument.parse("{id: 1234}")))
        );
    }

    @Test
    @DisplayName("when 'after' field empty in value doc then DataException")
    void testEmptyAfterFieldInValueDocument() {
        assertThrows(DataException.class, () ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument("op", new BsonString("UPDATE"))
                        ,BsonDocument.parse("{id: 1234}").append("after", new BsonDocument())))
        );
    }

    @Test
    @DisplayName("when 'after' field null in value doc then DataException")
    void testNullAfterFieldInValueDocument() {
        assertThrows(DataException.class, () ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument("op", new BsonString("u")),
                        BsonDocument.parse("{id: 1234}").append("after", new BsonNull())))
        );
    }

    @Test
    @DisplayName("when 'after' field no document in value doc then DataException")
    void testNoDocumentAfterFieldInValueDocument() {
        assertThrows(DataException.class, () ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument("op", new BsonString("UPDATE"))
                        ,BsonDocument.parse("{id: 1234}").append("after", new BsonString("wrong type"))))
        );
    }

    @Test
    @DisplayName("when key doc and value 'before' field both empty then DataException")
    void testEmptyKeyDocAndEmptyValueBeforeField() {
        assertThrows(DataException.class, () ->
                RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument(),
                        new BsonDocument("before", new BsonDocument())))
        );
    }

}
