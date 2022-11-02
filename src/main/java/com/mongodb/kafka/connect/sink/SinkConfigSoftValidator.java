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
package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DELETE_ON_NULL_VALUES_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DELETE_ON_NULL_VALUES_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_UUID_FORMAT_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_MAPPING_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_MAPPING_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_REGEXP_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_REGEXP_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_LIST_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_TYPE_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.POST_PROCESSOR_CHAIN_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_LIST_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_TYPE_DEFAULT;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.WRITEMODEL_STRATEGY_CONFIG;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.mongodb.kafka.connect.util.config.ConfigSoftValidator;
import com.mongodb.kafka.connect.util.config.ConfigSoftValidator.IncompatiblePropertiesPair;
import com.mongodb.kafka.connect.util.config.ConfigSoftValidator.ObsoletePropertiesSet;

final class SinkConfigSoftValidator {
  private static final Set<ObsoletePropertiesSet> OBSOLETE_PROPERTIES =
      unmodifiableSet(
          Stream.of(
                  ObsoletePropertiesSet.unused(
                      Stream.of("max.num.retries", "retries.defer.timeout").collect(toSet()),
                      format(
                          "The sink connector started to rely on retries in the MongoDB Java driver."
                              + " Remove the property from your configuration as it has no effect."
                              + " If you have 'retryWrites=false' specified in the '%1$s' configuration property,"
                              + " then retries are disabled for the sink connector;"
                              + " remove 'retryWrites=false' from '%1$s' if you want to enable retries.",
                          MongoSinkConfig.CONNECTION_URI_CONFIG)))
              .collect(toSet()));

  private static final Set<IncompatiblePropertiesPair> INCOMPATIBLE_PROPERTIES =
      unmodifiableSet(
          Stream.of(
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      POST_PROCESSOR_CHAIN_CONFIG,
                      null),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      FIELD_RENAMER_MAPPING_CONFIG,
                      FIELD_RENAMER_MAPPING_DEFAULT),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      FIELD_RENAMER_REGEXP_CONFIG,
                      FIELD_RENAMER_REGEXP_DEFAULT),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      KEY_PROJECTION_LIST_CONFIG,
                      KEY_PROJECTION_LIST_DEFAULT),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      KEY_PROJECTION_TYPE_CONFIG,
                      KEY_PROJECTION_TYPE_DEFAULT),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      VALUE_PROJECTION_LIST_CONFIG,
                      VALUE_PROJECTION_LIST_DEFAULT),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      VALUE_PROJECTION_TYPE_CONFIG,
                      VALUE_PROJECTION_TYPE_DEFAULT),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      WRITEMODEL_STRATEGY_CONFIG,
                      null),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      DOCUMENT_ID_STRATEGY_CONFIG,
                      null),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_CONFIG,
                      String.valueOf(DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_DEFAULT)),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      DOCUMENT_ID_STRATEGY_UUID_FORMAT_CONFIG,
                      null),
                  IncompatiblePropertiesPair.latterIgnored(
                      CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                      CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                      DELETE_ON_NULL_VALUES_CONFIG,
                      String.valueOf(DELETE_ON_NULL_VALUES_DEFAULT)))
              .collect(toSet()));

  /** @see ConfigSoftValidator#logObsoleteProperties(Set, Collection, Consumer) */
  static void logObsoleteProperties(
      final Collection<String> propertyNames, final Consumer<String> logger) {
    ConfigSoftValidator.logObsoleteProperties(OBSOLETE_PROPERTIES, propertyNames, logger);
  }

  /** @see ConfigSoftValidator#logIncompatibleProperties(Set, Map, Consumer) */
  static void logIncompatibleProperties(
      final Map<String, String> props, final Consumer<String> logger) {
    ConfigSoftValidator.logIncompatibleProperties(INCOMPATIBLE_PROPERTIES, props, logger);
  }

  private SinkConfigSoftValidator() {}
}
