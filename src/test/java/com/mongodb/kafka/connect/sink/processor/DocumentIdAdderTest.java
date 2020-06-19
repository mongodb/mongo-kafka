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

package com.mongodb.kafka.connect.sink.processor;

import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class DocumentIdAdderTest {

    @Test
    @DisplayName("test default IdStrategy")
    void testDefaultIdFieldStrategy() {
        SinkDocument sinkDocWithValueDoc = new SinkDocument(null, new BsonDocument());

        new DocumentIdAdder(createTopicConfig()).process(sinkDocWithValueDoc, null);
        assertAll("check for _id field when processing DocumentIdAdder",
                () -> assertTrue(sinkDocWithValueDoc.getValueDoc().orElseGet(BsonDocument::new).containsKey("_id"),
                        "must contain _id field in valueDoc"),
                () -> assertNotNull(sinkDocWithValueDoc.getValueDoc().orElseGet(BsonDocument::new).get("_id"),
                        "_id field must be of type BsonValue")
        );
    }

    @Test
    @DisplayName("test default IdStrategy handles null values")
    void testDefaultIdFieldStrategyNullValues() {
        SinkDocument sinkDocWithoutValueDoc = new SinkDocument(null, null);
        new DocumentIdAdder(createTopicConfig()).process(sinkDocWithoutValueDoc, null);
        assertFalse(sinkDocWithoutValueDoc.getValueDoc().isPresent(), "no _id added since valueDoc was not");
    }

    @Test
    @DisplayName("test DocumentIdAdder obeys the overwrite existing configuration")
    void testDocumentIdAdderOverwriteExistingConfiguration() {
        SinkDocument sinkDocWithValueDoc = new SinkDocument(new BsonDocument(), BsonDocument.parse("{_id: 1}"));
        new DocumentIdAdder(createTopicConfig()).process(sinkDocWithValueDoc, null);

        assertTrue(sinkDocWithValueDoc.getValueDoc().orElseGet(BsonDocument::new).get("_id").isInt32(),
                "default id strategy ignores existing _id values");

        new DocumentIdAdder(createTopicConfig(MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_CONFIG, "true"))
                .process(sinkDocWithValueDoc, null);

        assertTrue(sinkDocWithValueDoc.getValueDoc().orElseGet(BsonDocument::new).get("_id").isObjectId(), "_id has new value");
    }

}
