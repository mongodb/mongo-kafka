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

package com.mongodb.kafka.connect.sink.processor.id.strategy;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_UUID_FORMAT_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.BlacklistKeyProjector;
import com.mongodb.kafka.connect.sink.processor.BlacklistValueProjector;
import com.mongodb.kafka.connect.sink.processor.WhitelistKeyProjector;
import com.mongodb.kafka.connect.sink.processor.WhitelistValueProjector;

@RunWith(JUnitPlatform.class)
class IdStrategyTest {
    private static final int UUID_STRING_LENGTH = 36;
    private static final int BSON_OID_STRING_LENGTH = 12;
    private static final int KAFKA_META_DATA_PARTS = 3;

    @TestFactory
    @DisplayName("test different id generation strategies")
    List<DynamicTest> testIdGenerationStrategies() {
        List<DynamicTest> idTests = new ArrayList<>();

        IdStrategy idS1 = new BsonOidStrategy();
        idTests.add(dynamicTest(BsonOidStrategy.class.getSimpleName(), () -> {
            BsonValue id = idS1.generateId(null, null);
            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonObjectId),
                    () -> assertEquals(BSON_OID_STRING_LENGTH, ((BsonObjectId) id).getValue().toByteArray().length)
            );
        }));

        UuidStrategy idS2 = new UuidStrategy();
        idTests.add(dynamicTest(UuidStrategy.class.getSimpleName(), () -> {

            idS2.configure(createTopicConfig());
            BsonValue id = idS2.generateId(null, null);
            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(UUID_STRING_LENGTH, id.asString().getValue().length())
            );

            idS2.configure(createTopicConfig(DOCUMENT_ID_STRATEGY_UUID_FORMAT_CONFIG, "Binary"));
            BsonValue id2 = idS2.generateId(null, null);
            assertAll("id checks",
                    () -> assertTrue(id2.isBinary()),
                    () -> assertDoesNotThrow(() -> id2.asBinary().asUuid())
            );
        }));

        IdStrategy idS3 = new ProvidedInKeyStrategy();
        idTests.add(dynamicTest(ProvidedStrategy.class.getSimpleName() + " in key", () -> {
            String idValue = "SOME_UNIQUE_ID_IN_KEY";
            SinkDocument sdWithIdInKeyDoc = new SinkDocument(new BsonDocument("_id", new BsonString(idValue)), null);
            SinkDocument sdWithoutIdInKeyDoc = new SinkDocument(new BsonDocument(), null);
            SinkDocument sdWithBsonNullIdInKeyDoc = new SinkDocument(new BsonDocument("_id", BsonNull.VALUE), null);
            BsonValue id = idS3.generateId(sdWithIdInKeyDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(idValue, id.asString().getValue())
            );
            assertThrows(DataException.class, () -> idS3.generateId(sdWithoutIdInKeyDoc, null));
            assertThrows(DataException.class, () -> idS3.generateId(sdWithBsonNullIdInKeyDoc, null));
        }));

        IdStrategy idS4 = new ProvidedInValueStrategy();
        idTests.add(dynamicTest(ProvidedStrategy.class.getSimpleName() + " in value", () -> {
            String idValue = "SOME_UNIQUE_ID_IN_VALUE";
            SinkDocument sdWithIdInValueDoc = new SinkDocument(null, new BsonDocument("_id", new BsonString(idValue)));
            SinkDocument sdWithoutIdInValueDoc = new SinkDocument(null, new BsonDocument());
            SinkDocument sdWithBsonNullIdInValueDoc = new SinkDocument(null, new BsonDocument());
            BsonValue id = idS4.generateId(sdWithIdInValueDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(idValue, id.asString().getValue())
            );
            assertThrows(DataException.class, () -> idS4.generateId(sdWithoutIdInValueDoc, null));
            assertThrows(DataException.class, () -> idS4.generateId(sdWithBsonNullIdInValueDoc, null));
        }));

        IdStrategy idS5 = new KafkaMetaDataStrategy();
        idTests.add(dynamicTest(KafkaMetaDataStrategy.class.getSimpleName(), () -> {
            String topic = "some-topic";
            int partition = 1234;
            long offset = 9876543210L;
            SinkRecord sr = new SinkRecord(topic, partition, null, null, null, null, offset);
            BsonValue id = idS5.generateId(null, sr);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> {
                        String[] parts = id.asString().getValue().split(KafkaMetaDataStrategy.DELIMITER);
                        assertAll("meta data checks",
                                () -> assertEquals(KAFKA_META_DATA_PARTS, parts.length),
                                () -> assertEquals(topic, parts[0]),
                                () -> assertEquals(partition, Integer.parseInt(parts[1])),
                                () -> assertEquals(offset, Long.parseLong(parts[2]))
                        );
                    }
            );
        }));

        IdStrategy idS6 = new FullKeyStrategy();
        idTests.add(dynamicTest(FullKeyStrategy.class.getSimpleName(), () -> {
            BsonDocument keyDoc = new BsonDocument() {{
                put("myInt", new BsonInt32(123));
                put("myString", new BsonString("ABC"));
            }};
            SinkDocument sdWithKeyDoc = new SinkDocument(keyDoc, null);
            SinkDocument sdWithoutKeyDoc = new SinkDocument(null, null);
            BsonValue id = idS6.generateId(sdWithKeyDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonDocument),
                    () -> assertEquals(keyDoc, id.asDocument())
            );
            assertEquals(new BsonDocument(), idS6.generateId(sdWithoutKeyDoc, null));
        }));
        return idTests;
    }

    @Test
    @DisplayName("test PartialKeyStrategy with blacklisting")
    void testPartialKeyStrategyBlacklist() {
        BsonDocument keyDoc = BsonDocument.parse("{keyPart1: 123, keyPart2: 'ABC', keyPart3: true}");
        BsonDocument expected = BsonDocument.parse("{keyPart2: 'ABC', keyPart3: true}");

        MongoSinkTopicConfig cfg = createTopicConfig(format("{'%s': 'blacklist', '%s': 'keyPart1'}",
                KEY_PROJECTION_TYPE_CONFIG, KEY_PROJECTION_LIST_CONFIG));

        IdStrategy ids = new PartialKeyStrategy(new BlacklistKeyProjector(cfg));
        SinkDocument sd = new SinkDocument(keyDoc, null);
        BsonValue id = ids.generateId(sd, null);

        assertAll("id checks",
                () -> assertTrue(id instanceof BsonDocument),
                () -> assertEquals(expected, id.asDocument())
        );
        assertEquals(new BsonDocument(), ids.generateId(new SinkDocument(null, null), null));
    }

    @Test
    @DisplayName("test PartialKeyStrategy with whitelisting")
    void testPartialKeyStrategyWhitelist() {
        BsonDocument keyDoc = BsonDocument.parse("{keyPart1: 123, keyPart2: 'ABC', keyPart3: true}");
        BsonDocument expected = BsonDocument.parse("{keyPart1: 123}");

        MongoSinkTopicConfig cfg = createTopicConfig(format("{'%s': 'whitelist', '%s': 'keyPart1'}",
                KEY_PROJECTION_TYPE_CONFIG, KEY_PROJECTION_LIST_CONFIG));

        IdStrategy ids = new PartialKeyStrategy(new WhitelistKeyProjector(cfg));
        SinkDocument sd = new SinkDocument(keyDoc, null);
        BsonValue id = ids.generateId(sd, null);

        assertAll("id checks",
                () -> assertTrue(id instanceof BsonDocument),
                () -> assertEquals(expected, id.asDocument())
        );
        assertEquals(new BsonDocument(), ids.generateId(new SinkDocument(null, null), null));
    }

    @Test
    @DisplayName("test PartialValueStrategy with blacklisting")
    void testPartialValueStrategyBlacklist() {
        BsonDocument valueDoc = BsonDocument.parse("{valuePart1: 123, valuePart2: 'ABC', valuePart3: true}");
        BsonDocument expected = BsonDocument.parse("{valuePart2: 'ABC', valuePart3: true}");

        MongoSinkTopicConfig cfg = createTopicConfig(format("{'%s': 'blacklist', '%s': 'valuePart1'}",
                VALUE_PROJECTION_TYPE_CONFIG, VALUE_PROJECTION_LIST_CONFIG));

        IdStrategy ids = new PartialValueStrategy(new BlacklistValueProjector(cfg));
        SinkDocument sd = new SinkDocument(null, valueDoc);
        BsonValue id = ids.generateId(sd, null);

        assertAll("id checks",
                () -> assertTrue(id instanceof BsonDocument),
                () -> assertEquals(expected, id.asDocument())
        );
        assertEquals(new BsonDocument(), ids.generateId(new SinkDocument(null, null), null));
    }

    @Test
    @DisplayName("test PartialValueStrategy with whitelisting")
    void testPartialValueStrategyWhitelist() {
        BsonDocument valueDoc = BsonDocument.parse("{valuePart1: 123, valuePart2: 'ABC', valuePart3: true}");
        BsonDocument expected = BsonDocument.parse("{valuePart1: 123}");

        MongoSinkTopicConfig cfg = createTopicConfig(format("{'%s': 'whitelist', '%s': 'valuePart1'}",
                VALUE_PROJECTION_TYPE_CONFIG, VALUE_PROJECTION_LIST_CONFIG));

        IdStrategy ids = new PartialValueStrategy(new WhitelistValueProjector(cfg));
        SinkDocument sd = new SinkDocument(null, valueDoc);
        BsonValue id = ids.generateId(sd, null);

        assertAll("id checks",
                () -> assertTrue(id instanceof BsonDocument),
                () -> assertEquals(expected, id.asDocument())
        );
        assertEquals(new BsonDocument(), ids.generateId(new SinkDocument(null, null), null));
    }
}
