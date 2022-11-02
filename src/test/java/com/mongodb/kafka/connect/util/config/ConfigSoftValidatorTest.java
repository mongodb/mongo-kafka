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
package com.mongodb.kafka.connect.util.config;

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPIC_OVERRIDE_CONFIG;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.mongodb.lang.Nullable;

import com.mongodb.kafka.connect.util.config.ConfigSoftValidator.IncompatiblePropertiesPair;
import com.mongodb.kafka.connect.util.config.ConfigSoftValidator.ObsoletePropertiesSet;

final class ConfigSoftValidatorTest {
  @Test
  void testObsoleteProperties() {
    Set<ObsoletePropertiesSet> obsoleteConfigs =
        Stream.of(
                ObsoletePropertiesSet.unused(Stream.of("a", "b").collect(toSet()), null),
                ObsoletePropertiesSet.unused(Stream.of("c").collect(toSet()), "Unused."),
                ObsoletePropertiesSet.deprecated("d", "f", null),
                ObsoletePropertiesSet.deprecated("e", "f", "Deprecated."),
                ObsoletePropertiesSet.deprecated("g", "h", null),
                ObsoletePropertiesSet.deprecated("i", "k", null))
            .collect(toSet());
    Set<String> props =
        Stream.of(
                "a",
                "b",
                "c",
                topicOverridePropertyName("t", "c"),
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "l")
            .collect(toSet());
    Set<String> expectedMessages =
        Stream.of(
                unusedMsg("a", null),
                unusedMsg("b", null),
                unusedMsg("c", "Unused."),
                unusedMsg(topicOverridePropertyName("t", "c"), "Unused."),
                deprecatedMsg("d", "f", null),
                deprecatedMsg("e", "f", "Deprecated."),
                deprecatedMsg("g", "h", null),
                deprecatedMsg("i", "k", null))
            .collect(toSet());
    Set<String> actualMessages = new HashSet<>();
    ConfigSoftValidator.logObsoleteProperties(obsoleteConfigs, props, actualMessages::add);
    assertEquals(expectedMessages, actualMessages);
  }

  @Test
  void logIncompatibleProperties() {
    Set<IncompatiblePropertiesPair> incompatibleConfigs =
        Stream.of(
                IncompatiblePropertiesPair.latterIgnored("a", "", "c", null),
                IncompatiblePropertiesPair.latterIgnored("c", null, "b", ""),
                IncompatiblePropertiesPair.latterIgnored("d", "defaultD", "e", "defaultE"),
                IncompatiblePropertiesPair.latterIgnored("d", "defaultD", "f", "defaultF"))
            .collect(Collectors.toSet());
    Map<String, String> props = new HashMap<>();
    props.put("a", "valueA");
    props.put("b", "valueB");
    props.put("c", "");
    props.put(topicOverridePropertyName("t", "c"), "valueC");
    props.put("d", "valueD");
    props.put("e", "valueE");
    props.put("f", "valueF");
    props.put("g", "valueG");
    Set<String> expectedMessages =
        Stream.of(
                latterIgnoredMsg("a", "c"),
                latterIgnoredMsg("a", topicOverridePropertyName("t", "c")),
                latterIgnoredMsg("c", "b"),
                latterIgnoredMsg(topicOverridePropertyName("t", "c"), "b"),
                latterIgnoredMsg("d", "e"),
                latterIgnoredMsg("d", "f"))
            .collect(toSet());
    Set<String> actualMessages = new HashSet<>();
    ConfigSoftValidator.logIncompatibleProperties(incompatibleConfigs, props, actualMessages::add);
    assertEquals(expectedMessages, actualMessages);
  }

  private static String topicOverridePropertyName(
      final String topicName, final String propertyName) {
    return format(TOPIC_OVERRIDE_CONFIG, topicName, propertyName);
  }

  private static String unusedMsg(final String propertyName, @Nullable final String msg) {
    return format(
        "The configuration property '%s' is unused.%s", propertyName, msg == null ? "" : " " + msg);
  }

  private static String deprecatedMsg(
      final String propertyName, final String newPropertyName, @Nullable final String msg) {
    return format(
        "The configuration property '%s' is deprecated. Use '" + newPropertyName + "' instead.%s",
        propertyName,
        msg == null ? "" : " " + msg);
  }

  private static String latterIgnoredMsg(final String propertyName1, final String propertyName2) {
    return format(
        "Configuration property '%s' is incompatible with '%s'. The latter is ignored.",
        propertyName1, propertyName2);
  }
}
