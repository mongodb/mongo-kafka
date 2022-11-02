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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.mongodb.kafka.connect.util.config.ConfigSoftValidator;
import com.mongodb.kafka.connect.util.config.ConfigSoftValidator.IncompatiblePropertiesPair;
import com.mongodb.kafka.connect.util.config.ConfigSoftValidator.ObsoletePropertiesSet;

final class SourceConfigSoftValidator {
  private static final Set<ObsoletePropertiesSet> OBSOLETE_PROPERTIES = Collections.emptySet();

  private static final Set<IncompatiblePropertiesPair> INCOMPATIBLE_PROPERTIES =
      Collections.emptySet();

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
