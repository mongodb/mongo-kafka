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
package com.mongodb.kafka.connect.util;

import static com.mongodb.kafka.connect.util.FlexibleDateTimeParser.DEFAULT_DATE_TIME_FORMATTER_PATTERN;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.format.DateTimeParseException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class FlexibleDateTimeParserTest {

  private static final FlexibleDateTimeParser DEFAULT_DATE_TIME_PARSER =
      new FlexibleDateTimeParser(DEFAULT_DATE_TIME_FORMATTER_PATTERN, "");

  @Test
  @DisplayName("test default date string")
  void testDefaultDateStrings() {
    assertAll(
        () ->
            assertEquals(
                3600000,
                DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T01:00:00.000+00:00[GMT]")),
        () ->
            assertEquals(
                3600000,
                DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01 02:00:00 +01:00[Europe/Paris]")),
        () ->
            assertEquals(
                3600000,
                DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T02:00:00+01:00[Europe/Paris]")),
        () ->
            assertEquals(
                3600000, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T00:00:00.000001-0100")),
        () ->
            assertEquals(
                3600001, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T00:00:00.001-0100")),
        () ->
            assertEquals(
                3600000, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T00:00:00-0100")),
        () ->
            assertEquals(
                3600000, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T01:00:00.000001Z")),
        () ->
            assertEquals(
                3600001, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T01:00:00.001Z")),
        () -> assertEquals(3600000, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T01:00:00Z")),
        () ->
            assertEquals(3600001, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-0101:00:00.001")),
        () -> assertEquals(3600000, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-0101:00:00")),
        () ->
            assertEquals(
                3600000, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01 00:00:00 -0100")),
        () -> assertEquals(86400000, DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-02")));
  }

  @Test
  @DisplayName("test errors parsing invalid strings")
  void testErrorsParsingInvalidStrings() {
    assertAll(
        () ->
            assertThrows(
                DateTimeParseException.class,
                () -> DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970"),
                "Missing month and day"),
        () ->
            assertThrows(
                DateTimeParseException.class,
                () -> DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01"),
                "Missing day"),
        () ->
            assertThrows(
                DateTimeParseException.class,
                () -> DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T00:01"),
                "Missing Seconds"),
        () ->
            assertThrows(
                DateTimeParseException.class,
                () -> DEFAULT_DATE_TIME_PARSER.toEpochMilli("epoch"),
                "Normal string"),
        () ->
            assertThrows(
                DateTimeParseException.class,
                () -> DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01 -0100"),
                "Offset with no time data"),
        () ->
            assertThrows(
                DateTimeParseException.class,
                () ->
                    DEFAULT_DATE_TIME_PARSER.toEpochMilli("1970-01-01T02:00:00+01:00[Asia/Paris]"),
                "Invalid Zone"),
        () ->
            assertThrows(
                DateTimeParseException.class,
                () -> new FlexibleDateTimeParser("QQQ y", "").toEpochMilli("Q1 2008"),
                "Missing month and day"));
  }

  @Test
  @DisplayName("Alternative date time formats")
  void testAlternativeDateTimeTormats() {
    assertEquals(
        3600000,
        new FlexibleDateTimeParser("EEEE, MMM dd, yyyy HH:mm:ss", "en")
            .toEpochMilli("Thursday, Jan 01, 1970 01:00:00"));
  }
}
