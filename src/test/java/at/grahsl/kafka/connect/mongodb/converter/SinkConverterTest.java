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

package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.*;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
public class SinkConverterTest {

    public static String JSON_STRING_1;
    public static Schema OBJ_SCHEMA_1;
    public static Struct OBJ_STRUCT_1;
    public static Map OBJ_MAP_1;
    public static BsonDocument EXPECTED_BSON_DOC;

    private static Map<Object, Schema> combinations;
    private SinkConverter sinkConverter = new SinkConverter();


    @BeforeAll
    public static void initializeTestData() {

        JSON_STRING_1 = "{\"myField\":\"some text\"}";

        OBJ_SCHEMA_1 = SchemaBuilder.struct()
                .field("myField", Schema.STRING_SCHEMA);

        OBJ_STRUCT_1 = new Struct(OBJ_SCHEMA_1)
                .put("myField", "some text");

        OBJ_MAP_1 = new LinkedHashMap<>();
        OBJ_MAP_1.put("myField", "some text");

        EXPECTED_BSON_DOC = new BsonDocument("myField", new BsonString("some text"));

        combinations = new HashMap<>();
        combinations.put(JSON_STRING_1, null);
        combinations.put(OBJ_STRUCT_1, OBJ_SCHEMA_1);
        combinations.put(OBJ_MAP_1, null);
    }

    @TestFactory
    @DisplayName("test different combinations for sink record conversions")
    public List<DynamicTest> testDifferentOptionsForSinkRecordConversion() {

        List<DynamicTest> tests = new ArrayList<>();

        for (Map.Entry<Object, Schema> entry : combinations.entrySet()) {

            tests.add(dynamicTest("key only SinkRecord conversion for type " + entry.getKey().getClass().getName()
                    + " with data -> " + entry.getKey(), () -> {
                SinkDocument converted = sinkConverter.convert(
                        new SinkRecord(
                                "topic", 1, entry.getValue(), entry.getKey(), null, null, 0L
                        )
                );
                assertAll("checks on conversion results",
                        () -> assertNotNull(converted),
                        () -> assertEquals(EXPECTED_BSON_DOC, converted.getKeyDoc().get()),
                        () -> assertEquals(Optional.empty(), converted.getValueDoc())
                );
            }));

            tests.add(dynamicTest("value only SinkRecord conversion for type " + entry.getKey().getClass().getName()
                    + " with data -> " + entry.getKey(), () -> {
                SinkDocument converted = sinkConverter.convert(
                        new SinkRecord(
                                "topic", 1, null, null, entry.getValue(), entry.getKey(), 0L
                        )
                );
                assertAll("checks on conversion results",
                        () -> assertNotNull(converted),
                        () -> assertEquals(Optional.empty(), converted.getKeyDoc()),
                        () -> assertEquals(EXPECTED_BSON_DOC, converted.getValueDoc().get())
                );
            }));

            tests.add(dynamicTest("key + value SinkRecord conversion for type " + entry.getKey().getClass().getName()
                    + " with data -> " + entry.getKey(), () -> {
                SinkDocument converted = sinkConverter.convert(
                        new SinkRecord(
                                "topic", 1, entry.getValue(), entry.getKey(), entry.getValue(), entry.getKey(), 0L
                        )
                );
                assertAll("checks on conversion results",
                        () -> assertNotNull(converted),
                        () -> assertEquals(EXPECTED_BSON_DOC, converted.getKeyDoc().get()),
                        () -> assertEquals(EXPECTED_BSON_DOC, converted.getValueDoc().get())
                );
            }));

        }

        return tests;

    }

    @Test
    @DisplayName("test empty sink record conversion")
    public void testEmptySinkRecordConversion() {

        SinkDocument converted = sinkConverter.convert(
                new SinkRecord(
                        "topic", 1, null, null, null, null, 0L
                )
        );

        assertAll("checks on conversion result",
                () -> assertNotNull(converted),
                () -> assertEquals(Optional.empty(), converted.getKeyDoc()),
                () -> assertEquals(Optional.empty(), converted.getValueDoc())
        );

    }

    @Test
    @DisplayName("test invalid sink record conversion")
    public void testInvalidSinkRecordConversion() {

        assertAll("checks on conversion result",
                () -> assertThrows(DataException.class, () -> sinkConverter.convert(
                        new SinkRecord(
                                "topic", 1, null, new Object(), null, null, 0L
                        )
                )),
                () -> assertThrows(DataException.class, () -> sinkConverter.convert(
                        new SinkRecord(
                                "topic", 1, null, null, null, new Object(), 0L
                        )
                )),
                () -> assertThrows(DataException.class, () -> sinkConverter.convert(
                        new SinkRecord(
                                "topic", 1, null, new Object(), null, new Object(), 0L
                        )
                ))
        );

    }

}
