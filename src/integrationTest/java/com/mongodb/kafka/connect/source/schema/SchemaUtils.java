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

import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

import junit.framework.AssertionFailedError;

public final class SchemaUtils {

  public static void assertStructsEquals(final List<Struct> expected, final List<Struct> actual) {
    assertEquals(
        expected.size(),
        actual.size(),
        format("Different sized lists: expected %s, actual: %s", expected.size(), actual.size()));

    for (int i = 0; i < expected.size(); i++) {
      Struct expectedStruct = expected.get(i);
      Struct actualStruct = actual.get(i);

      try {
        assertStructEquals(expectedStruct, actualStruct);
      } catch (Throwable e) {
        throw new AssertionFailedError(
            format("Struct differed in position [%s] : %s", i, e.getMessage()));
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

  static void assertSchemaEquals(final Schema expected, final Schema actual) {
    assertEquals(expected.isOptional(), actual.isOptional(), "Optional value differs");
    assertEquals(expected.version(), actual.version(), "version differs");
    assertEquals(expected.name(), actual.name(), "name differs");

    assertEquals(expected.doc(), actual.doc(), "docs differ");
    assertEquals(expected.type(), actual.type(), "type differs");

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
  }

  private SchemaUtils() {}
}
