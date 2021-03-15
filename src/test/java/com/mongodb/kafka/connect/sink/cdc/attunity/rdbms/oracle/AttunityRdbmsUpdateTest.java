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
 */

package com.mongodb.kafka.connect.sink.cdc.attunity.rdbms.oracle;

import com.mongodb.client.model.UpdateOneModel;
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
    private static final BsonDocument FILTER_DOC =
            BsonDocument.parse("{_id: {table: 1234, key: 43214}}");
    private static final BsonDocument AFTER_DOC =
            BsonDocument.parse("{first_name: 'Grace', last_name: 'Hopper'}");
    private static final BsonDocument HEADER_DOC = BsonDocument.parse("{operation: 'UPDATE'}");
    private static final BsonDocument BEFORE_DOC =
            BsonDocument.parse("{first_name: 'Julie', last_name: 'NotHooper'}");
    private static final BsonDocument UPDATE_DOC =
            BsonDocument.parse("{ $set: {first_name: 'Grace', last_name: 'Hopper'}}");

    @Test
    @DisplayName("when valid doc change cdc event then correct UpdateOneModel")
    void testValidSinkDocumentForUpdate() {
        BsonDocument keyDoc = BsonDocument.parse("{table: 1234, key: 43214}");
        BsonDocument valueDoc =
                new BsonDocument(
                        "message",
                        new BsonDocument("data", AFTER_DOC)
                                .append("headers", HEADER_DOC)
                                .append("beforeData", BEFORE_DOC));

        WriteModel<BsonDocument> result = RDBMS_UPDATE.perform(new SinkDocument(keyDoc, valueDoc));
        assertTrue(result instanceof UpdateOneModel, "result expected to be of type UpdateOneModel");

        UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;
        assertEquals(UPDATE_DOC, writeModel.getUpdate(), "update doc not matching what is expected");
        assertTrue(
                writeModel.getFilter() instanceof BsonDocument,
                "filter expected to be of type BsonDocument");
        assertEquals(FILTER_DOC, writeModel.getFilter());
    }

    @Test
    @DisplayName("when missing value doc then DataException")
    void testMissingValueDocument() {
        assertThrows(
                DataException.class,
                () -> RDBMS_UPDATE.perform(new SinkDocument(new BsonDocument(), null)));
    }

    @Test
    @DisplayName("when missing key doc then DataException")
    void testMissingKeyDocument() {
        assertThrows(
                DataException.class,
                () ->
                        RDBMS_UPDATE.perform(
                                new SinkDocument(
                                        null,
                                        BsonDocument.parse("{message: { data: {}, beforeData: {}, headers: {}}}"))));
    }

    @Test
    @DisplayName("when 'update' field missing in value doc then DataException")
    void testMissingPatchFieldInValueDocument() {
        assertThrows(
                DataException.class,
                () ->
                        RDBMS_UPDATE.perform(
                                new SinkDocument(
                                        BsonDocument.parse("{id: 1234}"),
                                        BsonDocument.parse(
                                                "{message: { data: {}, beforeData: {}, headers: {operation: ''}}}"))));
    }

    @Test
    @DisplayName("when 'id' field invalid JSON in key doc then DataException")
    void testIdFieldInvalidJsonInKeyDocument() {
        assertThrows(
                DataException.class,
                () ->
                        RDBMS_UPDATE.perform(
                                new SinkDocument(
                                        BsonDocument.parse("{id: '{not-Json}'}"),
                                        BsonDocument.parse("{message: { data: {}, beforeData: {}, headers: {}}}"))));
    }

    @Test
    @DisplayName("test Optional.empty() when no-op update is received")
    void testForNoOpBehavior() {
        BsonDocument keyDoc = BsonDocument.parse("{table: 1234, key: 43214}");
        BsonDocument valueDoc =
                new BsonDocument(
                        "message",
                        new BsonDocument(
                                "data", BsonDocument.parse("{first_name: 'Grace', last_name: 'Hopper'}"))
                                .append("headers", BsonDocument.parse("{operation: 'UPDATE'}"))
                                .append(
                                        "beforeData",
                                        BsonDocument.parse("{first_name: 'Grace', last_name: 'Hopper'}")));

        WriteModel<BsonDocument> result = RDBMS_UPDATE.perform(new SinkDocument(keyDoc, valueDoc));
        assertNull(result, "result expected to be null");
    }
}
