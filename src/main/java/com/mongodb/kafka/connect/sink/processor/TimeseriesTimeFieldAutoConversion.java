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
package com.mongodb.kafka.connect.sink.processor;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_AUTO_CONVERSION_DATE_FORMAT_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_AUTO_CONVERSION_LOCALE_LANGUAGE_TAG_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_CONFIG;
import static java.lang.String.format;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDateTime;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.util.FlexibleDateTimeParser;

class TimeseriesTimeFieldAutoConversion extends PostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessor.class);

  private final String fieldName;
  private final FlexibleDateTimeParser flexibleDateTimeParser;

  TimeseriesTimeFieldAutoConversion(final MongoSinkTopicConfig config) {
    super(config);
    fieldName = config.getString(TIMESERIES_TIMEFIELD_CONFIG);
    flexibleDateTimeParser =
        new FlexibleDateTimeParser(
            config.getString(TIMESERIES_TIMEFIELD_AUTO_CONVERSION_DATE_FORMAT_CONFIG),
            config.getString(TIMESERIES_TIMEFIELD_AUTO_CONVERSION_LOCALE_LANGUAGE_TAG_CONFIG));
  }

  @Override
  public void process(final SinkDocument doc, final SinkRecord orig) {
    doc.getValueDoc()
        .filter(d -> d.containsKey(fieldName) && (d.isNumber(fieldName) || d.isString(fieldName)))
        .ifPresent(
            d -> {
              BsonValue timeField = d.get(fieldName);
              Optional<BsonDateTime> convertedValue;
              if (timeField.isNumber()) {
                convertedValue =
                    tryToConvert(() -> new BsonDateTime(timeField.asNumber().longValue()));
              } else {
                convertedValue =
                    tryToConvert(
                        () ->
                            new BsonDateTime(
                                flexibleDateTimeParser.toEpochMilli(
                                    timeField.asString().getValue())));
              }
              convertedValue.map(bsonDateTime -> d.put(fieldName, bsonDateTime));
            });
  }

  Optional<BsonDateTime> tryToConvert(final Supplier<BsonDateTime> supplier) {
    try {
      return Optional.of(supplier.get());
    } catch (Exception e) {
      LOGGER.info(
          format(
              "Failed to convert field `%s` to a valid date time, so leaving as is: `%s`",
              fieldName, e.getMessage()));
      return Optional.empty();
    }
  }
}
