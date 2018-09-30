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

import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.*;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.DateFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.DecimalFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.TimeFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.TimestampFieldConverter;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.bson.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
public class SinkFieldConverterTest {

    @TestFactory
    @DisplayName("tests for boolean field conversions")
    public List<DynamicTest> testBooleanFieldConverter() {

        SinkFieldConverter converter = new BooleanFieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(true,false)).forEach(el -> {
            tests.add(dynamicTest("conversion with "
                    + converter.getClass().getSimpleName() + " for "+el,
                    () -> assertEquals(el, ((BsonBoolean)converter.toBson(el)).getValue())
            ));
        });

        tests.add(dynamicTest("optional type conversion checks", () -> {
            Schema valueOptionalDefault = SchemaBuilder.bool().optional().defaultValue(true);
            assertAll("",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.BOOLEAN_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_BOOLEAN_SCHEMA)),
                    () -> assertEquals(valueOptionalDefault.defaultValue(),
                            converter.toBson(null, valueOptionalDefault).asBoolean().getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for int8 field conversions")
    public List<DynamicTest> testInt8FieldConverter() {

        SinkFieldConverter converter = new Int8FieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(Byte.MIN_VALUE,(byte)0,Byte.MAX_VALUE)).forEach(
                el -> tests.add(dynamicTest("conversion with "
                            + converter.getClass().getSimpleName() + " for "+el,
                    () -> assertEquals((int)el, ((BsonInt32)converter.toBson(el)).getValue())
            ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.int8().optional().defaultValue((byte)0);
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.INT8_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_INT8_SCHEMA)),
                    () -> assertEquals(((Byte)valueOptionalDefault.defaultValue()).intValue(),
                            ((BsonInt32)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for int16 field conversions")
    public List<DynamicTest> testInt16FieldConverter() {

        SinkFieldConverter converter = new Int16FieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(Short.MIN_VALUE,(short)0,Short.MAX_VALUE)).forEach(
                el -> tests.add(dynamicTest("conversion with "
                            + converter.getClass().getSimpleName() + " for "+el,
                    () -> assertEquals((short)el, ((BsonInt32)converter.toBson(el)).getValue())
            ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.int16().optional().defaultValue((short)0);
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.INT16_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_INT16_SCHEMA)),
                    () -> assertEquals(((short)valueOptionalDefault.defaultValue()),
                            ((BsonInt32)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for int32 field conversions")
    public List<DynamicTest> testInt32FieldConverter() {

        SinkFieldConverter converter = new Int32FieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(Integer.MIN_VALUE,0,Integer.MAX_VALUE)).forEach(
                el -> tests.add(dynamicTest("conversion with "
                            + converter.getClass().getSimpleName() + " for "+el,
                    () -> assertEquals((int)el, ((BsonInt32)converter.toBson(el)).getValue())
            ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.int32().optional().defaultValue(0);
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.INT32_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_INT32_SCHEMA)),
                    () -> assertEquals(valueOptionalDefault.defaultValue(),
                            ((BsonInt32)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for int64 field conversions")
    public List<DynamicTest> testInt64FieldConverter() {

        SinkFieldConverter converter = new Int64FieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(Long.MIN_VALUE,0L,Long.MAX_VALUE)).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals((long)el, ((BsonInt64)converter.toBson(el)).getValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.int64().optional().defaultValue(0L);
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.INT64_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_INT64_SCHEMA)),
                    () -> assertEquals((long)valueOptionalDefault.defaultValue(),
                            ((BsonInt64)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for float32 field conversions")
    public List<DynamicTest> testFloat32FieldConverter() {

        SinkFieldConverter converter = new Float32FieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(Float.MIN_VALUE,0f,Float.MAX_VALUE)).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals((float)el, ((BsonDouble)converter.toBson(el)).getValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.float32().optional().defaultValue(0.0f);
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.FLOAT32_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_FLOAT32_SCHEMA)),
                    () -> assertEquals(((Float)valueOptionalDefault.defaultValue()).doubleValue(),
                            ((BsonDouble)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for float64 field conversions")
    public List<DynamicTest> testFloat64FieldConverter() {

        SinkFieldConverter converter = new Float64FieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(Double.MIN_VALUE,0d,Double.MAX_VALUE)).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals((double)el, ((BsonDouble)converter.toBson(el)).getValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.float64().optional().defaultValue(0.0d);
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.FLOAT64_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_FLOAT64_SCHEMA)),
                    () -> assertEquals(valueOptionalDefault.defaultValue(),
                            ((BsonDouble)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for string field conversions")
    public List<DynamicTest> testStringFieldConverter() {

        SinkFieldConverter converter = new StringFieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList("fooFOO","","blahBLAH")).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals(el, ((BsonString)converter.toBson(el)).getValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.string().optional().defaultValue("");
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.STRING_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_STRING_SCHEMA)),
                    () -> assertEquals(valueOptionalDefault.defaultValue(),
                            ((BsonString)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for bytes field conversions based on byte[]")
    public List<DynamicTest> testBytesFieldConverterByteArray() {

        SinkFieldConverter converter = new BytesFieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(new byte[]{-128,-127,0},new byte[]{},new byte[]{0,126,127})).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+Arrays.toString(el),
                        () -> assertEquals(el, ((BsonBinary)converter.toBson(el)).getData())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.bytes().optional().defaultValue(new byte[]{});
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.BYTES_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_BYTES_SCHEMA)),
                    () -> assertEquals(valueOptionalDefault.defaultValue(),
                            ((BsonBinary)converter.toBson(null, valueOptionalDefault)).getData())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for bytes field conversions based on ByteBuffer")
    public List<DynamicTest> testBytesFieldConverterByteBuffer() {

        SinkFieldConverter converter = new BytesFieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(ByteBuffer.wrap(new byte[]{-128,-127,0}),
                                        ByteBuffer.wrap(new byte[]{}),
                                        ByteBuffer.wrap(new byte[]{0,126,127}))).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el.toString()
                                    +" -> "+Arrays.toString(el.array()),
                        () -> assertEquals(el.array(), ((BsonBinary)converter.toBson(el)).getData())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = SchemaBuilder.bytes().optional().defaultValue(ByteBuffer.wrap(new byte[]{}));
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Schema.BYTES_SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Schema.OPTIONAL_BYTES_SCHEMA)),
                    () -> assertEquals(((ByteBuffer)valueOptionalDefault.defaultValue()).array(),
                            ((BsonBinary)converter.toBson(null, valueOptionalDefault)).getData())
            );
        }));

        return tests;

    }

    @Test
    @DisplayName("tests for bytes field conversions with invalid type")
    public void testBytesFieldConverterInvalidType() {
        assertThrows(DataException.class, () -> new BytesFieldConverter().toBson(new Object()));
    }

    @TestFactory
    @DisplayName("tests for logical type date field conversions")
    public List<DynamicTest> testDateFieldConverter() {

        SinkFieldConverter converter = new DateFieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(
            java.util.Date.from(ZonedDateTime.of(LocalDate.of(1970,1,1), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant()),
            java.util.Date.from(ZonedDateTime.of(LocalDate.of(1983,7,31), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant()),
            java.util.Date.from(ZonedDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant())
        )).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals(el.toInstant().getEpochSecond()*1000,
                                                ((BsonDateTime)converter.toBson(el)).getValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = Date.builder().optional().defaultValue(
                java.util.Date.from(ZonedDateTime.of(LocalDate.of(1970,1,1), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant())
            );
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Date.SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Date.builder().optional())),
                    () -> assertEquals(((java.util.Date)valueOptionalDefault.defaultValue()).toInstant().getEpochSecond()*1000,
                            ((BsonDateTime)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for logical type time field conversions")
    public List<DynamicTest> testTimeFieldConverter() {

        SinkFieldConverter converter = new TimeFieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(
                java.util.Date.from(ZonedDateTime.of(LocalDate.of(1970,1,1), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant()),
                java.util.Date.from(ZonedDateTime.of(LocalDate.of(1970,1,1), LocalTime.NOON, ZoneOffset.systemDefault()).toInstant())
        )).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals(el.toInstant().getEpochSecond()*1000,
                                ((BsonDateTime)converter.toBson(el)).getValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = Time.builder().optional().defaultValue(
                    java.util.Date.from(ZonedDateTime.of(LocalDate.of(1970,1,1), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant())
            );
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Time.SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Time.builder().optional())),
                    () -> assertEquals(((java.util.Date)valueOptionalDefault.defaultValue()).toInstant().getEpochSecond()*1000,
                            ((BsonDateTime)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for logical type timestamp field conversions")
    public List<DynamicTest> testTimestampFieldConverter() {

        SinkFieldConverter converter = new TimestampFieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(
                java.util.Date.from(ZonedDateTime.of(LocalDate.of(1970,1,1), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant()),
                java.util.Date.from(ZonedDateTime.of(LocalDate.of(1983,7,31), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant()),
                java.util.Date.from(ZonedDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant())
        )).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals(el.toInstant().getEpochSecond()*1000,
                                ((BsonDateTime)converter.toBson(el)).getValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = Timestamp.builder().optional().defaultValue(
                    java.util.Date.from(ZonedDateTime.of(LocalDate.of(1970,1,1), LocalTime.MIDNIGHT, ZoneOffset.systemDefault()).toInstant())
            );
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Timestamp.SCHEMA)),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Timestamp.builder().optional())),
                    () -> assertEquals(((java.util.Date)valueOptionalDefault.defaultValue()).toInstant().getEpochSecond()*1000,
                            ((BsonDateTime)converter.toBson(null, valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for logical type decimal field conversions (new)")
    public List<DynamicTest> testDecimalFieldConverterNew() {

        SinkFieldConverter converter = new DecimalFieldConverter();

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(
            new BigDecimal("-1234567890.09876543210"),
                BigDecimal.ZERO,
            new BigDecimal("+1234567890.09876543210")
        )).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals(el,
                                ((BsonDecimal128)converter.toBson(el)).getValue().bigDecimalValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = Decimal.builder(0).optional().defaultValue(BigDecimal.ZERO);
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Decimal.schema(0))),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Decimal.builder(0).optional())),
                    () -> assertEquals(valueOptionalDefault.defaultValue(),
                            ((BsonDecimal128)converter.toBson(null,valueOptionalDefault)).getValue().bigDecimalValue())
            );
        }));

        return tests;

    }

    @TestFactory
    @DisplayName("tests for logical type decimal field conversions (legacy)")
    public List<DynamicTest> testDecimalFieldConverterLegacy() {

        SinkFieldConverter converter =
                new DecimalFieldConverter(DecimalFieldConverter.Format.LEGACYDOUBLE);

        List<DynamicTest> tests = new ArrayList<>();
        new ArrayList<>(Arrays.asList(
                new BigDecimal("-1234567890.09876543210"),
                BigDecimal.ZERO,
                new BigDecimal("+1234567890.09876543210")
        )).forEach(
                el -> tests.add(dynamicTest("conversion with "
                                + converter.getClass().getSimpleName() + " for "+el,
                        () -> assertEquals(el.doubleValue(),
                                ((BsonDouble)converter.toBson(el)).getValue())
                ))
        );

        tests.add(dynamicTest("optional type conversions", () -> {
            Schema valueOptionalDefault = Decimal.builder(0).optional().defaultValue(BigDecimal.ZERO);
            assertAll("checks",
                    () -> assertThrows(DataException.class, () -> converter.toBson(null, Decimal.schema(0))),
                    () -> assertEquals(new BsonNull(), converter.toBson(null, Decimal.builder(0).optional())),
                    () -> assertEquals(((BigDecimal)valueOptionalDefault.defaultValue()).doubleValue(),
                            ((BsonDouble)converter.toBson(null,valueOptionalDefault)).getValue())
            );
        }));

        return tests;

    }

    @Test
    @DisplayName("tests for logical type decimal field conversions (invalid)")
    public void testDecimalFieldConverterInvalidType() {
        assertThrows(DataException.class, () -> new DecimalFieldConverter().toBson(new Object()));
    }

}
