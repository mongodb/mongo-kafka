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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public class SimpleDateTimeParser {

  private final DateTimeFormatter formatter;

  public SimpleDateTimeParser(final String dateTimePattern) {
    this.formatter = DateTimeFormatter.ofPattern(dateTimePattern);
  }

  public long toEpochMilli(final String dateTimeString) {
    TemporalAccessor parsed = formatter.parse(dateTimeString);
    if (parsed.isSupported(ChronoField.INSTANT_SECONDS)) {
      return Instant.from(parsed).toEpochMilli();
    }
    return LocalDateTime.from(parsed).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
  }
}
