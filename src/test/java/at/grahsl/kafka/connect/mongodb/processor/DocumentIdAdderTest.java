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

package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.id.strategy.IdStrategy;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitPlatform.class)
class DocumentIdAdderTest {

    @Test
    @DisplayName("test _id field added by IdStrategy")
    void testAddingIdFieldByStrategy() {
        BsonValue fakeId = mock(BsonValue.class);
        IdStrategy ids = mock(IdStrategy.class);
        when(ids.generateId(any(SinkDocument.class), ArgumentMatchers.isNull())).thenReturn(fakeId);

        DocumentIdAdder idAdder = new DocumentIdAdder(null, ids, "");
        SinkDocument sinkDocWithValueDoc = new SinkDocument(null, new BsonDocument());
        SinkDocument sinkDocWithoutValueDoc = new SinkDocument(null, null);

        assertAll("check for _id field when processing DocumentIdAdder",
                () -> {
                    idAdder.process(sinkDocWithValueDoc, null);
                    assertAll("_id checks",
                            () -> assertTrue(sinkDocWithValueDoc.getValueDoc().orElseGet(BsonDocument::new).keySet().contains("_id"),
                                    "must contain _id field in valueDoc"),
                            () -> assertNotNull(sinkDocWithValueDoc.getValueDoc().orElseGet(BsonDocument::new).get("_id"),
                                    "_id field must be of type BsonValue"));
                },
                () -> {
                    idAdder.process(sinkDocWithoutValueDoc, null);
                    assertTrue(!sinkDocWithoutValueDoc.getValueDoc().isPresent(), "no _id added since valueDoc cannot not be present");
                }
        );
    }
}
