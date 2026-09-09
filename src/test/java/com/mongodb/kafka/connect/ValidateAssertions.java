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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.kafka.common.config.Config;

/** Assertions shared by connector config-validation tests. */
final class ValidateAssertions {

  private ValidateAssertions() {}

  static void assertSecretAbsent(final Config config, final String secret) {
    boolean leaked =
        config.configValues().stream()
            .anyMatch(
                configValue ->
                    configValue.value() != null
                            && String.valueOf(configValue.value()).contains(secret)
                        || configValue.errorMessages().stream().anyMatch(m -> m.contains(secret))
                        || configValue.recommendedValues().stream()
                            .anyMatch(r -> r != null && String.valueOf(r).contains(secret)));
    assertFalse(
        leaked, "Resolved secret leaked in the validate() response: " + config.configValues());
  }
}
