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

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.util.Validators.ValidatorWithOperators;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.mongodb.kafka.connect.util.Validators;

@RunWith(JUnitPlatform.class)
class ValidatorWithOperatorsTest {
  private static final String NAME = "name";
  private static final Object ANY_VALUE = null;

  private static final ValidatorWithOperators PASS =
      (name, value) -> {
        // ignore, always passes
      };

  private static final ValidatorWithOperators FAIL =
      (name, value) -> {
        throw new ConfigException(name, value, "always fails");
      };

  @Test
  @DisplayName("validate empty string")
  void emptyString() {
    ValidatorWithOperators validator = Validators.emptyString();
    validator.ensureValid(NAME, "");
  }

  @Test
  @DisplayName("invalidate non-empty string")
  void invalidateNonEmptyString() {
    ValidatorWithOperators validator = Validators.emptyString();
    assertThrows(ConfigException.class, () -> validator.ensureValid(NAME, "value"));
  }

  @Test
  @DisplayName("validate regex")
  void simpleRegex() {
    ValidatorWithOperators validator = Validators.matching(Pattern.compile("fo+ba[rz]"));
    validator.ensureValid(NAME, "foobar");
    validator.ensureValid(NAME, "foobaz");
  }

  @Test
  @DisplayName("invalidate regex")
  void invalidateSimpleRegex() {
    ValidatorWithOperators validator = Validators.matching(Pattern.compile("fo+ba[rz]"));
    assertThrows(ConfigException.class, () -> validator.ensureValid(NAME, "foobax"));
    assertThrows(ConfigException.class, () -> validator.ensureValid(NAME, "fbar"));
  }

  @Test
  @DisplayName("validate arithmetic or")
  void arithmeticOr() {
    PASS.or(PASS).ensureValid(NAME, ANY_VALUE);
    PASS.or(FAIL).ensureValid(NAME, ANY_VALUE);
    FAIL.or(PASS).ensureValid(NAME, ANY_VALUE);
    PASS.or(PASS).or(PASS).ensureValid(NAME, ANY_VALUE);
    PASS.or(PASS).or(FAIL).ensureValid(NAME, ANY_VALUE);
    PASS.or(FAIL).or(PASS).ensureValid(NAME, ANY_VALUE);
    PASS.or(FAIL).or(FAIL).ensureValid(NAME, ANY_VALUE);
    FAIL.or(PASS).or(PASS).ensureValid(NAME, ANY_VALUE);
    FAIL.or(PASS).or(FAIL).ensureValid(NAME, ANY_VALUE);
    FAIL.or(FAIL).or(PASS).ensureValid(NAME, ANY_VALUE);
  }

  @Test
  @DisplayName("invalidate arithmetic or")
  void invalidateArithmeticOr() {
    assertThrows(ConfigException.class, () -> FAIL.or(FAIL).ensureValid(NAME, ANY_VALUE));
    assertThrows(ConfigException.class, () -> FAIL.or(FAIL).or(FAIL).ensureValid(NAME, ANY_VALUE));
  }
}
