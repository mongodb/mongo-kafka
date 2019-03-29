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

package com.mongodb.kafka.connect.sink.converter;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;
import org.bson.types.Decimal128;

@RunWith(JUnitPlatform.class)
class RecordConverterTest {
    private static String jsonString1;
    private static Schema objSchema1;
    private static Struct objStruct1;
    private static Map<String, Object> objMap1;
    private static BsonDocument expectedBsonDocBytes1;
    private static BsonDocument expectedBsonDocRaw1;

    @BeforeAll
    static void initializeTestData() {
        jsonString1 = "{\"_id\":\"1234567890\","
                + "\"myString\":\"some foo bla text\","
                + "\"myInt\":42,"
                + "\"myBoolean\":true,"
                + "\"mySubDoc1\":{\"myString\":\"hello json\"},"
                + "\"myArray1\":[\"str_1\",\"str_2\",\"...\",\"str_N\"],"
                + "\"myArray2\":[{\"k\":\"a\",\"v\":1},{\"k\":\"b\",\"v\":2},{\"k\":\"c\",\"v\":3}],"
                + "\"mySubDoc2\":{\"k1\":9,\"k2\":8,\"k3\":7},"
                + "\"myBytes\":\"S2Fma2Egcm9ja3Mh\","
                + "\"myDate\": 1489705200000,"
                + "\"myTimestamp\": 1489705200000,"
                + "\"myTime\": 946724400000, "
                + "\"myDecimal\": 12345.6789 }";

        objSchema1 = SchemaBuilder.struct()
                .field("_id", Schema.STRING_SCHEMA)
                .field("myString", Schema.STRING_SCHEMA)
                .field("myInt", Schema.INT32_SCHEMA)
                .field("myBoolean", Schema.BOOLEAN_SCHEMA)
                .field("mySubDoc1", SchemaBuilder.struct().field("myString", Schema.STRING_SCHEMA).build())
                .field("myArray1", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("myArray2", SchemaBuilder.array(
                        SchemaBuilder.struct().field("k", Schema.STRING_SCHEMA).field("v", Schema.INT32_SCHEMA).build()))
                .field("mySubDoc2", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .field("myBytes", Schema.BYTES_SCHEMA)
                .field("myDate", Date.SCHEMA)
                .field("myTimestamp", Timestamp.SCHEMA)
                .field("myTime", Time.SCHEMA)
                .field("myDecimal", Decimal.schema(0))
                .build();

        objStruct1 = new Struct(objSchema1)
                .put("_id", "1234567890")
                .put("myString", "some foo bla text")
                .put("myInt", 42)
                .put("myBoolean", true)
                .put("mySubDoc1", new Struct(objSchema1.field("mySubDoc1").schema()).put("myString", "hello json"))
                .put("myArray1", asList("str_1", "str_2", "...", "str_N"))
                .put("myArray2", asList(
                        new Struct(objSchema1.field("myArray2").schema().valueSchema()).put("k", "a").put("v", 1),
                        new Struct(objSchema1.field("myArray2").schema().valueSchema()).put("k", "b").put("v", 2),
                        new Struct(objSchema1.field("myArray2").schema().valueSchema()).put("k", "c").put("v", 3))
                )
                .put("mySubDoc2", new HashMap<String, Integer>() {{
                    put("k1", 9);
                    put("k2", 8);
                    put("k3", 7);
                }})
                .put("myBytes", new byte[]{75, 97, 102, 107, 97, 32, 114, 111, 99, 107, 115, 33})
                .put("myDate", java.util.Date.from(ZonedDateTime.of(
                        LocalDate.of(2017, 3, 17), LocalTime.MIDNIGHT, ZoneOffset.UTC).toInstant()))
                .put("myTimestamp", java.util.Date.from(ZonedDateTime.of(
                        LocalDate.of(2017, 3, 17), LocalTime.MIDNIGHT, ZoneOffset.UTC).toInstant()))
                .put("myTime", java.util.Date.from(ZonedDateTime.of(
                        LocalDate.of(2000, 1, 1), LocalTime.NOON, ZoneOffset.UTC).toInstant()))
                .put("myDecimal", new BigDecimal("12345.6789"));

        objMap1 = new LinkedHashMap<String, Object>() {{
            put("_id", "1234567890");
            put("myString", "some foo bla text");
            put("myInt", 42);
            put("myBoolean", true);
            put("mySubDoc1", new HashMap<Object, Object>() {{
                put("myString", "hello json");
            }});
            put("myArray1", asList("str_1", "str_2", "...", "str_N"));
            put("myArray2", asList(
                    new HashMap<Object, Object>() {{
                        put("k", "a");
                        put("v", 1);
                    }},
                    new HashMap<Object, Object>() {{
                        put("k", "b");
                        put("v", 2);
                    }},
                    new HashMap<Object, Object>() {{
                        put("k", "c");
                        put("v", 3);
                    }}));
            put("mySubDoc2", new HashMap<String, Integer>() {{
                put("k1", 9);
                put("k2", 8);
                put("k3", 7);
            }});
            put("myBytes", new byte[]{75, 97, 102, 107, 97, 32, 114, 111, 99, 107, 115, 33});
            put("myDate", java.util.Date.from(ZonedDateTime.of(
                    LocalDate.of(2017, 3, 17), LocalTime.MIDNIGHT, ZoneOffset.UTC).toInstant()));
            put("myTimestamp", java.util.Date.from(ZonedDateTime.of(LocalDate.of(2017, 3, 17),
                    LocalTime.MIDNIGHT, ZoneOffset.UTC).toInstant()));
            put("myTime", java.util.Date.from(ZonedDateTime.of(
                    LocalDate.of(2000, 1, 1), LocalTime.NOON, ZoneOffset.UTC).toInstant()));
            //NOTE: as of now the BSON codec package seems to be missing a BigDecimalCodec
            // thus I'm cheating a little by using a Decimal128 here...
            put("myDecimal", Decimal128.parse("12345.6789"));
        }};

        expectedBsonDocBytes1 = BsonDocument.parse("{_id: '1234567890', myString: 'some foo bla text', myInt: 42, myBoolean: true, "
                + "mySubDoc1: {myString: 'hello json'}, myArray1: ['str_1', 'str_2', '...', 'str_N'], "
                + "myArray2: [{k: 'a', v: 1}, {k: 'b', v: 2}, {k: 'c', v: 3}], mySubDoc2: {k1: 9, k2: 8, k3: 7}, "
                + "myBytes: {$binary: 'S2Fma2Egcm9ja3Mh', $type: '00'}, myDate: {$date: 1489708800000}, "
                + "myTimestamp: {$date: 1489708800000}, myTime: {$date: 946728000000}, myDecimal: {$numberDecimal: '12345.6789'}}");

        expectedBsonDocRaw1 = BsonDocument.parse("{_id: '1234567890', myString: 'some foo bla text', myInt: 42, myBoolean: true, "
                + "mySubDoc1: {myString: 'hello json'}, myArray1: ['str_1', 'str_2', '...', 'str_N'], "
                + "myArray2: [{k: 'a', v: 1}, {k: 'b', v: 2}, {k: 'c', v: 3}], mySubDoc2: {k1: 9, k2: 8, k3: 7}, "
                + "myBytes: 'S2Fma2Egcm9ja3Mh', myDate: {$numberLong: '1489705200000'}, "
                + "myTimestamp: {$numberLong: '1489705200000'}, myTime: {$numberLong: '946724400000'}, 'myDecimal': 12345.6789}");
    }

    @Test
    @DisplayName("test raw json conversion")
    void testJsonRawStringConversion() {
        RecordConverter converter = new JsonRawStringRecordConverter();
        assertAll("",
                () -> assertEquals(expectedBsonDocRaw1, converter.convert(null, jsonString1)),
                () -> assertThrows(DataException.class, () -> converter.convert(null, null))
        );
    }

    @Test
    @DisplayName("test avro or (json + schema) conversion (which is handled the same)")
    void testAvroOrJsonWithSchemaConversion() {
        RecordConverter converter = new AvroJsonSchemafulRecordConverter();
        assertAll("",
                () -> assertEquals(expectedBsonDocBytes1, converter.convert(objSchema1, objStruct1)),
                () -> assertThrows(DataException.class, () -> converter.convert(objSchema1, null)),
                () -> assertThrows(DataException.class, () -> converter.convert(null, objStruct1)),
                () -> assertThrows(DataException.class, () -> converter.convert(null, null))
        );
    }

    @Test
    @DisplayName("test json object conversion")
    void testJsonObjectConversion() {
        RecordConverter converter = new JsonSchemalessRecordConverter();
        assertAll("",
                () -> assertEquals(expectedBsonDocBytes1, converter.convert(null, objMap1)),
                () -> assertThrows(DataException.class, () -> converter.convert(null, null))
        );
    }
}
