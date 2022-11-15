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
package com.mongodb.kafka.connect.source;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_ALLOW_DISK_USE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_ALLOW_DISK_USE_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_MAX_THREADS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_MAX_THREADS_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_NAMESPACE_REGEX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_NAMESPACE_REGEX_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_PIPELINE_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_QUEUE_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_QUEUE_SIZE_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_CONFIG_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_MAX_THREADS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_MAX_THREADS_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_PIPELINE_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_QUEUE_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_QUEUE_SIZE_DEFAULT;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.mongodb.kafka.connect.util.config.ConfigSoftValidator;
import com.mongodb.kafka.connect.util.config.ConfigSoftValidator.IncompatiblePropertiesPair;
import com.mongodb.kafka.connect.util.config.ConfigSoftValidator.ObsoletePropertiesSet;

final class SourceConfigSoftValidator {
  private static final Set<ObsoletePropertiesSet> OBSOLETE_PROPERTIES =
      unmodifiableSet(
          Stream.of(
                  ObsoletePropertiesSet.deprecated(
                      COPY_EXISTING_CONFIG,
                      STARTUP_MODE_CONFIG,
                      format(
                          "'%1$s = false' / '%1$s = true' should be replaced with '%2$s = %3$s' / '%2$s = %4$s'."
                              + " All `copy.existing.*` properties should be replaced"
                              + " with the corresponding `startup.mode.copy.existing.*` properties.",
                          COPY_EXISTING_CONFIG,
                          STARTUP_MODE_CONFIG,
                          StartupMode.LATEST.propertyValue(),
                          StartupMode.COPY_EXISTING.propertyValue())),
                  ObsoletePropertiesSet.deprecated(
                      COPY_EXISTING_MAX_THREADS_CONFIG,
                      STARTUP_MODE_COPY_EXISTING_MAX_THREADS_CONFIG,
                      null),
                  ObsoletePropertiesSet.deprecated(
                      COPY_EXISTING_QUEUE_SIZE_CONFIG,
                      STARTUP_MODE_COPY_EXISTING_QUEUE_SIZE_CONFIG,
                      null),
                  ObsoletePropertiesSet.deprecated(
                      COPY_EXISTING_PIPELINE_CONFIG,
                      STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG,
                      null),
                  ObsoletePropertiesSet.deprecated(
                      COPY_EXISTING_NAMESPACE_REGEX_CONFIG,
                      STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_CONFIG,
                      null),
                  ObsoletePropertiesSet.deprecated(
                      COPY_EXISTING_ALLOW_DISK_USE_CONFIG,
                      STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_CONFIG,
                      null))
              .collect(toSet()));

  private static final Set<IncompatiblePropertiesPair> INCOMPATIBLE_PROPERTIES =
      unmodifiableSet(
          Stream.of(
                  IncompatiblePropertiesPair.latterIgnored(
                      STARTUP_MODE_CONFIG,
                      STARTUP_MODE_CONFIG_DEFAULT.propertyValue(),
                      COPY_EXISTING_CONFIG,
                      String.valueOf(COPY_EXISTING_DEFAULT)),
                  IncompatiblePropertiesPair.latterIgnored(
                      STARTUP_MODE_COPY_EXISTING_MAX_THREADS_CONFIG,
                      String.valueOf(STARTUP_MODE_COPY_EXISTING_MAX_THREADS_DEFAULT),
                      COPY_EXISTING_MAX_THREADS_CONFIG,
                      String.valueOf(COPY_EXISTING_MAX_THREADS_DEFAULT)),
                  IncompatiblePropertiesPair.latterIgnored(
                      STARTUP_MODE_COPY_EXISTING_QUEUE_SIZE_CONFIG,
                      String.valueOf(STARTUP_MODE_COPY_EXISTING_QUEUE_SIZE_DEFAULT),
                      COPY_EXISTING_QUEUE_SIZE_CONFIG,
                      String.valueOf(COPY_EXISTING_QUEUE_SIZE_DEFAULT)),
                  IncompatiblePropertiesPair.latterIgnored(
                      STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG,
                      STARTUP_MODE_COPY_EXISTING_PIPELINE_DEFAULT,
                      COPY_EXISTING_PIPELINE_CONFIG,
                      COPY_EXISTING_PIPELINE_DEFAULT),
                  IncompatiblePropertiesPair.latterIgnored(
                      STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_CONFIG,
                      STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_DEFAULT,
                      COPY_EXISTING_NAMESPACE_REGEX_CONFIG,
                      COPY_EXISTING_NAMESPACE_REGEX_DEFAULT),
                  IncompatiblePropertiesPair.latterIgnored(
                      STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_CONFIG,
                      String.valueOf(STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_DEFAULT),
                      COPY_EXISTING_ALLOW_DISK_USE_CONFIG,
                      String.valueOf(COPY_EXISTING_ALLOW_DISK_USE_DEFAULT)))
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

  private SourceConfigSoftValidator() {}
}
