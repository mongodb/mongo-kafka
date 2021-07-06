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

import static java.lang.String.format;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

public class FlexibleDateTimeParser {

  private static final String TIME = "HH:mm:ss";
  private static final String OPTIONAL_NANO_SECONDS = "[[.][SSSSSS][SSS]]";
  private static final String OPTIONAL_TIME_WITH_TIMEZONE =
      format("[%s%s[ ]VV[ ]'['VV']']", TIME, OPTIONAL_NANO_SECONDS);
  private static final String OPTIONAL_TIME_WITH_OFFSET =
      format("[%s%s[ ]X]", TIME, OPTIONAL_NANO_SECONDS);
  private static final String OPTIONAL_TIME = format("[%s%s]", TIME, OPTIONAL_NANO_SECONDS);

  public static final String DEFAULT_DATE_TIME_FORMATTER_PATTERN =
      "yyyy-MM-dd" // Year month day
          + "[['T'][ ]]" // optional T or space
          + OPTIONAL_TIME_WITH_TIMEZONE
          + OPTIONAL_TIME_WITH_OFFSET
          + OPTIONAL_TIME;

  private final DateTimeFormatter formatter;

  public FlexibleDateTimeParser(final String dateTimePattern, final String languageTag) {
    Locale locale = languageTag.isEmpty() ? Locale.ROOT : Locale.forLanguageTag(languageTag);
    this.formatter = DateTimeFormatter.ofPattern(dateTimePattern, locale);
  }

  public long toEpochMilli(final String dateTimeString) {
    TemporalAccessor parsed = formatter.parse(dateTimeString);
    if (parsed.isSupported(ChronoField.INSTANT_SECONDS)) {
      return Instant.from(parsed).toEpochMilli();
    } else if (parsed.isSupported(ChronoField.SECOND_OF_MINUTE)) {
      return LocalDateTime.from(parsed).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
    } else if (parsed.isSupported(ChronoField.DAY_OF_MONTH)) {
      return LocalDate.from(parsed).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }
    throw new DateTimeParseException("Unsupported date time string", dateTimeString, 0);
  }
}
