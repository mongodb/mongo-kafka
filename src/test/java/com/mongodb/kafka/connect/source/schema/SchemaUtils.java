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

package com.mongodb.kafka.connect.source.schema;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

import junit.framework.AssertionFailedError;

public final class SchemaUtils {

  public static void assertSchemaAndValueEquals(
      final SchemaAndValue expected, final SchemaAndValue actual) {
    assertSchemaEquals(expected.schema(), actual.schema());

    Object expectedValue = convertData(expected.value());
    Object actualValue = convertData(actual.value());

    if (expectedValue instanceof Iterable && actualValue instanceof Iterable) {
      assertIterableEquals((Iterable<?>) expectedValue, (Iterable<?>) actualValue);
    } else {
      assertEquals(expected.value(), actual.value(), "Values differed");
    }
  }

  private static Object convertData(final Object value) {
    if (value instanceof byte[]) {
      // Doing equals on byte[] just tests instance equals and not the actual values
      return getBytesData((byte[]) value);
    }

    if (value instanceof Struct) {
      // Doing equals on Struct just tests instance equals and not the actual values
      return getStructData((Struct) value);
    }

    if (value instanceof List<?>) {
      // List values can contain Structs which need converting
      return getListData((List<?>) value);
    }
    return value;
  }

  private static Object getBytesData(final byte[] value) {
    List<Object> listValue = new ArrayList<>();
    for (byte b : value) {
      listValue.add(b);
    }
    return listValue;
  }

  private static Object getStructData(final Struct value) {
    List<Object> structValues = new ArrayList<>();
    value.schema().fields().forEach(f -> structValues.add(convertData(value.get(f))));
    return structValues;
  }

  private static List<?> getListData(final List<?> value) {
    return value.stream().map(SchemaUtils::convertData).collect(Collectors.toList());
  }

  public static void assertSchemaEquals(final Schema expected, final Schema actual) {
    assertEquals(
        expected.isOptional(), actual.isOptional(), "Optional value differs: " + actual.schema());
    assertEquals(expected.version(), actual.version(), "version differs: " + actual.schema());

    assertEquals(expected.doc(), actual.doc(), "docs differ: " + actual.schema());
    assertEquals(expected.type(), actual.type(), "type differs: " + actual.schema());

    Object expectedDefaultValue = expected.defaultValue();
    Object actualDefaultValue = actual.defaultValue();
    if (expectedDefaultValue instanceof Struct && actualDefaultValue instanceof Struct) {
      assertStructEquals((Struct) expectedDefaultValue, (Struct) actualDefaultValue);
    } else {
      assertEquals(expectedDefaultValue, actualDefaultValue, "values differ");
    }
    if (expected.type() == Type.MAP) {
      assertEquals(expected.keySchema(), actual.keySchema(), "keySchema differs");
      assertEquals(expected.valueSchema(), actual.valueSchema(), "valueSchema differs");
    } else if (expected.type() == Type.STRUCT) {
      assertStructSchemaEquals(expected, actual);
    }
    assertEquals(expected.parameters(), actual.parameters(), "parameters differs");
    assertEquals(expected.name(), actual.name(), "name differs: " + actual.schema());
  }

  static void assertStructSchemaEquals(final Schema expected, final Schema actual) {
    assertEquals(
        expected.fields().size(),
        actual.fields().size(),
        format(
            "Different fields length: expected %s, actual: %s",
            expected.fields().size(), actual.fields().size()));
    for (int i = 0; i < expected.fields().size(); i++) {
      Field expectedField = expected.schema().fields().get(i);
      Field actualField = actual.schema().fields().get(i);

      assertEquals(expectedField.name(), actualField.name(), "Field name differs");
      assertEquals(expectedField.index(), actualField.index(), "Field index differs");

      try {
        assertSchemaEquals(expectedField.schema(), actualField.schema());
      } catch (Throwable e) {
        throw new AssertionFailedError(
            format("Field schema differed for: %s : %s", expectedField.name(), e.getMessage()));
      }
    }
  }

  static void assertStructEquals(final Struct expected, final Struct actual) {
    assertStructSchemaEquals(expected.schema(), actual.schema());
    for (Field field : expected.schema().fields()) {
      assertEquals(
          expected.get(field), actual.get(field), format("Field: '%s' differs", field.name()));
    }
  }

  private SchemaUtils() {}
}
