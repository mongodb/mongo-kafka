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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TOPIC_OVERRIDE_PREFIX;
import static com.mongodb.kafka.connect.util.Assertions.assertNotNull;
import static com.mongodb.kafka.connect.util.Assertions.assertTrue;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigDef;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

/** Logs information about potential configuration issues. */
public final class ConfigSoftValidator {
  /**
   * This method is used in our implementation of the {@link ConfigDef#validateAll(Map)} method
   * because the original implementation silently disregards any undefined configuration properties.
   *
   * @param propertyNames See the parameter description in {@link #strippedPropertyName(String)}.
   */
  public static void logObsoleteProperties(
      final Set<ObsoletePropertiesSet> obsoleteConfigs,
      final Collection<String> propertyNames,
      final Consumer<String> logger) {
    propertyNames.stream()
        .map(
            propertyName ->
                obsoleteConfigs.stream()
                    .map(obsolete -> obsolete.msg(propertyName))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(logger);
  }

  /** @param props Properties as they are passed to {@link ConfigDef#validateAll(Map)}. */
  public static void logIncompatibleProperties(
      final Set<IncompatiblePropertiesPair> incompatibleConfigs,
      final Map<String, String> props,
      final Consumer<String> logger) {
    // global props are considered as belonging to a topic named "" for simplicity
    String global = "";
    Map<String, Map<String, Optional<String>>> topicNameToItsStrippedProps =
        props.entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    propNameAndValue -> topicNameFromPropertyName(propNameAndValue.getKey()),
                    Collectors.toMap(
                        propNameAndValue -> strippedPropertyName(propNameAndValue.getKey()),
                        e -> Optional.ofNullable(e.getValue()))));

    Map<String, Optional<String>> globalProps =
        topicNameToItsStrippedProps.getOrDefault(global, Collections.emptyMap());
    topicNameToItsStrippedProps.remove(global);
    Map<String, Map<String, Entry<Optional<String>, Boolean>>> topicNameToCombinedStrippedProps =
        topicNameToItsStrippedProps.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    topicNameAndItsStrippedProps ->
                        combineProperties(
                            globalProps,
                            topicNameAndItsStrippedProps.getKey(),
                            topicNameAndItsStrippedProps.getValue())));

    Map<String, Entry<Optional<String>, Boolean>> globalPropsWithFalseFlags =
        combineProperties(globalProps, null, null);
    incompatibleConfigs.forEach(
        incompatiblePair -> {
          incompatiblePair.logIfPresent(null, globalPropsWithFalseFlags, logger);
          topicNameToCombinedStrippedProps.forEach(
              (topicName, combinedStrippedProps) ->
                  incompatiblePair.logIfPresent(
                      topicName.equals(global) ? null : topicName, combinedStrippedProps, logger));
        });
  }

  /**
   * @param globalProps Properties that are not topic-specific.
   * @param topicName If {@code null}, then just converts {@code globalProps} to the result {@link
   *     Map}.
   * @param topicStrippedProps {@code null} iff {@code topicName} is {@code null}.
   * @return A {@link Map} where keys are property names (stripped, because {@code
   *     topicStrippedProps} must contain only stripped property names), and values are pairs of the
   *     property value and a flag telling whether the property value came from {@code
   *     topicStrippedProps}, i.e., was overridden, or from {@code globalProps}.
   */
  private static Map<String, Entry<Optional<String>, Boolean>> combineProperties(
      final Map<String, Optional<String>> globalProps,
      @Nullable final String topicName,
      @Nullable final Map<String, Optional<String>> topicStrippedProps) {
    assertTrue((topicName == null) ^ (topicStrippedProps != null));
    Map<String, Entry<Optional<String>, Boolean>> combinedStrippedProps = new HashMap<>();
    globalProps.forEach(
        (propertyName, propertyValue) ->
            combinedStrippedProps.put(
                propertyName, new AbstractMap.SimpleImmutableEntry<>(propertyValue, false)));
    if (topicStrippedProps != null) {
      topicStrippedProps.forEach(
          (propertyName, propertyValue) ->
              combinedStrippedProps.put(
                  propertyName, new AbstractMap.SimpleImmutableEntry<>(propertyValue, true)));
    }
    return combinedStrippedProps;
  }

  /**
   * @param propertyName Either a normal configuration property name, or property name prefixed with
   *     {@link MongoSinkTopicConfig#TOPIC_OVERRIDE_PREFIX} and a topic name, as specified <a
   *     href="https://docs.mongodb.com/kafka-connector/master/sink-connector/configuration-properties/topic-override/">here</a>.
   */
  private static String strippedPropertyName(final String propertyName) {
    return propertyName.startsWith(TOPIC_OVERRIDE_PREFIX)
        ? propertyName.substring(propertyName.indexOf(".", TOPIC_OVERRIDE_PREFIX.length() + 1) + 1)
        : propertyName;
  }

  /** @param strippedPropertyName See {@link #strippedPropertyName(String)}. */
  private static String overriddenPropertyName(
      final String topicName, final String strippedPropertyName) {
    return TOPIC_OVERRIDE_PREFIX + topicName + "." + strippedPropertyName;
  }

  /**
   * @return {@code ""} if {@code propertyName} is not prefixed with {@link
   *     MongoSinkTopicConfig#TOPIC_OVERRIDE_PREFIX}, otherwise returns the topic name from {@code
   *     propertyName}.
   */
  private static String topicNameFromPropertyName(final String propertyName) {
    if (propertyName.startsWith(TOPIC_OVERRIDE_PREFIX)) {
      int topicNameStartIdx = TOPIC_OVERRIDE_PREFIX.length();
      return propertyName.substring(
          topicNameStartIdx, propertyName.indexOf(".", topicNameStartIdx));
    } else {
      return "";
    }
  }

  private ConfigSoftValidator() {}

  /** A description of a set of obsolete configuration properties. */
  @Immutable
  public static final class ObsoletePropertiesSet {
    private final Set<String> propertyNames;
    private final String msgFormat;

    private ObsoletePropertiesSet(final Set<String> propertyNames, final String msgFormat) {
      this.propertyNames = unmodifiableSet(new HashSet<>(propertyNames));
      this.msgFormat = assertNotNull(msgFormat);
    }

    public static ObsoletePropertiesSet deprecated(
        final String propertyName, final String newPropertyName, @Nullable final String msg) {
      return new ObsoletePropertiesSet(
          Stream.of(propertyName).collect(toSet()),
          "The configuration property '%s' is deprecated. Use '"
              + newPropertyName
              + "' instead."
              + (msg == null ? "" : " " + msg));
    }

    public static ObsoletePropertiesSet unused(
        final Set<String> propertyNames, @Nullable final String msg) {
      return new ObsoletePropertiesSet(
          propertyNames,
          "The configuration property '%s' is unused." + (msg == null ? "" : " " + msg));
    }

    private Optional<String> msg(final String propertyName) {
      return propertyNames.contains(strippedPropertyName(propertyName))
          ? Optional.of(format(msgFormat, propertyName))
          : Optional.empty();
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ObsoletePropertiesSet that = (ObsoletePropertiesSet) o;
      return propertyNames.equals(that.propertyNames);
    }

    @Override
    public int hashCode() {
      return propertyNames.hashCode();
    }

    @Override
    public String toString() {
      return "ObsoletePropertiesSet{" + "names=" + propertyNames + ", msg=" + msgFormat + '}';
    }
  }

  /** A description of a pair of incompatible configuration properties. */
  @Immutable
  public static final class IncompatiblePropertiesPair {
    private final String propertyName1;
    private final String defaultPropertyValue1;
    private final String propertyName2;
    private final String defaultPropertyValue2;
    private final String msg;

    /**
     * @param defaultPropertyValue1 Must be {@code null} if the actual default value is not a
     *     special value that can be used to disable a global property per topic by explicitly
     *     setting it to the default value.
     * @param defaultPropertyValue2 See {@code defaultPropertyValue2}.
     */
    private IncompatiblePropertiesPair(
        final String propertyName1,
        @Nullable final String defaultPropertyValue1,
        final String propertyName2,
        @Nullable final String defaultPropertyValue2,
        @Nullable final String msg) {
      this.propertyName1 = propertyName1;
      this.defaultPropertyValue1 = defaultPropertyValue1;
      this.propertyName2 = propertyName2;
      this.defaultPropertyValue2 = defaultPropertyValue2;
      this.msg = msg == null ? "" : " " + msg;
    }

    public static IncompatiblePropertiesPair latterIgnored(
        final String propertyName1,
        @Nullable final String defaultPropertyValue1,
        final String propertyName2,
        @Nullable final String defaultPropertyValue2) {
      return new IncompatiblePropertiesPair(
          propertyName1,
          defaultPropertyValue1,
          propertyName2,
          defaultPropertyValue2,
          "The latter is ignored.");
    }

    /**
     * @param topicName {@code null} if {@code combinedStrippedProps} represent global properties.
     */
    private void logIfPresent(
        @Nullable final String topicName,
        final Map<String, Entry<Optional<String>, Boolean>> combinedStrippedProps,
        final Consumer<String> logger) {
      Entry<Optional<String>, Boolean> property1ValueAndOverridden =
          combinedStrippedProps.get(propertyName1);
      if (property1ValueAndOverridden == null
          || property1ValueAndOverridden
              .getKey()
              .equals(Optional.ofNullable(defaultPropertyValue1))) {
        return;
      }
      Entry<Optional<String>, Boolean> property2ValueAndOverridden =
          combinedStrippedProps.get(propertyName2);
      if (property2ValueAndOverridden == null
          || property2ValueAndOverridden
              .getKey()
              .equals(Optional.ofNullable(defaultPropertyValue2))) {
        return;
      }
      logger.accept(
          format(
              "Configuration property '%s' is incompatible with '%s'.%s",
              property1ValueAndOverridden.getValue()
                  ? overriddenPropertyName(assertNotNull(topicName), propertyName1)
                  : propertyName1,
              property2ValueAndOverridden.getValue()
                  ? overriddenPropertyName(assertNotNull(topicName), propertyName2)
                  : propertyName2,
              msg));
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final IncompatiblePropertiesPair that = (IncompatiblePropertiesPair) o;
      return propertyName1.equals(that.propertyName1) && propertyName2.equals(that.propertyName2);
    }

    @Override
    public int hashCode() {
      return propertyName1.hashCode() + propertyName2.hashCode();
    }

    @Override
    public String toString() {
      return "IncompatiblePropertiesPair{"
          + "name1="
          + propertyName1
          + ", name2="
          + propertyName2
          + ", msg="
          + msg
          + '}';
    }
  }
}
