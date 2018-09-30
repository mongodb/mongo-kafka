/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.BlacklistKeyProjector;
import at.grahsl.kafka.connect.mongodb.processor.BlacklistValueProjector;
import at.grahsl.kafka.connect.mongodb.processor.WhitelistKeyProjector;
import at.grahsl.kafka.connect.mongodb.processor.WhitelistValueProjector;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitPlatform.class)
public class IdStrategyTest {

    public static final int UUID_STRING_LENGTH = 36;
    public static final int BSON_OID_STRING_LENGTH = 12;
    public static final int KAFKA_META_DATA_PARTS = 3;

    @TestFactory
    @DisplayName("test different id generation strategies")
    public List<DynamicTest> testIdGenerationStrategies() {

        List<DynamicTest> idTests = new ArrayList<>();

        IdStrategy idS1 = new BsonOidStrategy();
        idTests.add(dynamicTest(BsonOidStrategy.class.getSimpleName(), () -> {
            BsonValue id = idS1.generateId(null,null);
            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonObjectId),
                    () -> assertEquals(BSON_OID_STRING_LENGTH,((BsonObjectId) id).getValue().toByteArray().length)
            );
        }));

        IdStrategy idS2 = new UuidStrategy();
        idTests.add(dynamicTest(UuidStrategy.class.getSimpleName(), () -> {
            BsonValue id = idS2.generateId(null, null);
            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(UUID_STRING_LENGTH,id.asString().getValue().length())
            );
        }));

        IdStrategy idS3 = new ProvidedInKeyStrategy();
        idTests.add(dynamicTest(ProvidedStrategy.class.getSimpleName() + " in key", () -> {

            String idValue = "SOME_UNIQUE_ID_IN_KEY";

            SinkDocument sdWithIdInKeyDoc = new SinkDocument(
                    new BsonDocument("_id",new BsonString(idValue)),null);

            SinkDocument sdWithoutIdInKeyDoc = new SinkDocument(
                    new BsonDocument(),null);

            SinkDocument sdWithBsonNullIdInKeyDoc = new SinkDocument(
                    new BsonDocument("_id",BsonNull.VALUE),null);

            BsonValue id = idS3.generateId(sdWithIdInKeyDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(idValue, id.asString().getValue())
            );

            assertThrows(DataException.class,() -> idS3.generateId(sdWithoutIdInKeyDoc, null));
            assertThrows(DataException.class,() -> idS3.generateId(sdWithBsonNullIdInKeyDoc, null));

        }));

        IdStrategy idS4 = new ProvidedInValueStrategy();
        idTests.add(dynamicTest(ProvidedStrategy.class.getSimpleName() + " in value", () -> {

            String idValue = "SOME_UNIQUE_ID_IN_VALUE";

            SinkDocument sdWithIdInValueDoc = new SinkDocument(
                   null, new BsonDocument("_id",new BsonString(idValue)));

            SinkDocument sdWithoutIdInValueDoc = new SinkDocument(
                    null,new BsonDocument());

            SinkDocument sdWithBsonNullIdInValueDoc = new SinkDocument(
                    null,new BsonDocument());

            BsonValue id = idS4.generateId(sdWithIdInValueDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> assertEquals(idValue,id.asString().getValue())
            );

            assertThrows(DataException.class,() -> idS4.generateId(sdWithoutIdInValueDoc, null));
            assertThrows(DataException.class,() -> idS4.generateId(sdWithBsonNullIdInValueDoc, null));

        }));

        IdStrategy idS5 = new KafkaMetaDataStrategy();
        idTests.add(dynamicTest(KafkaMetaDataStrategy.class.getSimpleName(), () -> {
            String topic = "some-topic";
            int partition = 1234;
            long offset = 9876543210L;
            SinkRecord sr = new SinkRecord(topic, partition,null,null,null,null, offset);
            BsonValue id = idS5.generateId(null, sr);
            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonString),
                    () -> {
                        String[] parts = id.asString().getValue().split(KafkaMetaDataStrategy.DELIMITER);
                        assertAll("meta data checks",
                                () -> assertEquals(KAFKA_META_DATA_PARTS,parts.length),
                                () -> assertEquals(topic, parts[0]),
                                () -> assertEquals(partition, Integer.parseInt(parts[1])),
                                () -> assertEquals(offset, Long.parseLong(parts[2]))
                        );
                    }
            );
        }));

        IdStrategy idS6 = new FullKeyStrategy();
        idTests.add(dynamicTest(FullKeyStrategy.class.getSimpleName(), () -> {

            BsonDocument keyDoc = new BsonDocument();
            keyDoc.put("myInt", new BsonInt32(123));
            keyDoc.put("myString", new BsonString("ABC"));

            SinkDocument sdWithKeyDoc = new SinkDocument(keyDoc,null);
            SinkDocument sdWithoutKeyDoc = new SinkDocument(null,null);

            BsonValue id = idS6.generateId(sdWithKeyDoc, null);

            assertAll("id checks",
                    () -> assertTrue(id instanceof BsonDocument),
                    () -> assertEquals(keyDoc,id.asDocument())
            );

            assertEquals(new BsonDocument(),idS6.generateId(sdWithoutKeyDoc, null));
        }));

        return idTests;
    }

    @Test
    @DisplayName("test PartialKeyStrategy with blacklisting")
    public void testPartialKeyStrategyBlacklist() {

        BsonDocument keyDoc = new BsonDocument();
        keyDoc.put("keyPart1", new BsonInt32(123));
        keyDoc.put("keyPart2", new BsonString("ABC"));
        keyDoc.put("keyPart3", new BsonBoolean(true));

        BsonDocument partialBlacklisted = new BsonDocument();
        partialBlacklisted.put("keyPart2", new BsonString("ABC"));
        partialBlacklisted.put("keyPart3", new BsonBoolean(true));

        MongoDbSinkConnectorConfig cfg =
                mock(MongoDbSinkConnectorConfig.class);
        when(cfg.getKeyProjectionList("")).thenReturn(new HashSet<>(Arrays.asList("keyPart1")));
        when(cfg.isUsingBlacklistKeyProjection("")).thenReturn(true);

        IdStrategy ids = new PartialKeyStrategy(new BlacklistKeyProjector(cfg,""));
        SinkDocument sd = new SinkDocument(keyDoc,null);
        BsonValue id = ids.generateId(sd, null);

        assertAll("id checks",
                () -> assertTrue(id instanceof BsonDocument),
                () -> assertEquals(partialBlacklisted,id.asDocument())
        );

        assertEquals(new BsonDocument(),ids.generateId(new SinkDocument(null,null), null));

    }

    @Test
    @DisplayName("test PartialKeyStrategy with whitelisting")
    public void testPartialKeyStrategyWhitelist() {

        BsonDocument keyDoc = new BsonDocument();
        keyDoc.put("keyPart1", new BsonInt32(123));
        keyDoc.put("keyPart2", new BsonString("ABC"));
        keyDoc.put("keyPart3", new BsonBoolean(true));

        BsonDocument partialWhitelisted = new BsonDocument();
        partialWhitelisted.put("keyPart1", new BsonInt32(123));

        MongoDbSinkConnectorConfig cfg =
                mock(MongoDbSinkConnectorConfig.class);
        when(cfg.getKeyProjectionList("")).thenReturn(new HashSet<>(Arrays.asList("keyPart1")));
        when(cfg.isUsingWhitelistKeyProjection("")).thenReturn(true);

        IdStrategy idS = new PartialKeyStrategy(new WhitelistKeyProjector(cfg,""));
        SinkDocument sd = new SinkDocument(keyDoc,null);
        BsonValue id = idS.generateId(sd, null);

        assertAll("id checks PartialKeyStrategy with whitelisting",
                () -> assertTrue(id instanceof BsonDocument),
                () -> assertEquals(partialWhitelisted,id.asDocument())
        );

        assertEquals(new BsonDocument(),idS.generateId(new SinkDocument(null,null), null));

    }

    @Test
    @DisplayName("test PartialValueStrategy with blacklisting")
    public void testPartialValueStrategyBlacklist() {

        BsonDocument valueDoc = new BsonDocument();
        valueDoc.put("valuePart1", new BsonInt32(123));
        valueDoc.put("valuePart2", new BsonString("ABC"));
        valueDoc.put("valuePart3", new BsonBoolean(true));

        BsonDocument partialBlacklisted = new BsonDocument();
        partialBlacklisted.put("valuePart2", new BsonString("ABC"));
        partialBlacklisted.put("valuePart3", new BsonBoolean(true));

        HashSet<String> fields = new HashSet<>(Arrays.asList("valuePart1"));
        MongoDbSinkConnectorConfig cfg =
                mock(MongoDbSinkConnectorConfig.class);
        when(cfg.isUsingBlacklistValueProjection("")).thenReturn(true);

        IdStrategy ids = new PartialValueStrategy(
                                new BlacklistValueProjector(cfg,fields,
                                    c -> c.isUsingBlacklistValueProjection(""),""));

        SinkDocument sd = new SinkDocument(null,valueDoc);
        BsonValue id = ids.generateId(sd, null);

        assertAll("id checks",
                () -> assertTrue(id instanceof BsonDocument),
                () -> assertEquals(partialBlacklisted,id.asDocument())
        );

        assertEquals(new BsonDocument(),ids.generateId(new SinkDocument(null,null), null));

    }

    @Test
    @DisplayName("test PartialValueStrategy with whitelisting")
    public void testPartialValueStrategyWhitelist() {

        BsonDocument valueDoc = new BsonDocument();
        valueDoc.put("valuePart1", new BsonInt32(123));
        valueDoc.put("valuePart2", new BsonString("ABC"));
        valueDoc.put("valuePart3", new BsonBoolean(true));

        BsonDocument partialWhitelisted = new BsonDocument();
        partialWhitelisted.put("valuePart1", new BsonInt32(123));

        HashSet<String> fields = new HashSet<>(Arrays.asList("valuePart1"));
        MongoDbSinkConnectorConfig cfg =
                mock(MongoDbSinkConnectorConfig.class);
        when(cfg.isUsingWhitelistValueProjection("")).thenReturn(true);

        IdStrategy ids = new PartialValueStrategy(
                new WhitelistValueProjector(cfg,fields,
                        c -> c.isUsingWhitelistValueProjection(""),""));

        SinkDocument sd = new SinkDocument(null,valueDoc);
        BsonValue id = ids.generateId(sd, null);

        assertAll("id checks",
                () -> assertTrue(id instanceof BsonDocument),
                () -> assertEquals(partialWhitelisted,id.asDocument())
        );

        assertEquals(new BsonDocument(),ids.generateId(new SinkDocument(null,null), null));

    }

}
