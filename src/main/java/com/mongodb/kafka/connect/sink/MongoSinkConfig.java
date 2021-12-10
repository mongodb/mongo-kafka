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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TOPIC_OVERRIDE_PREFIX;
import static com.mongodb.kafka.connect.util.ServerApiConfig.addServerApiConfig;
import static com.mongodb.kafka.connect.util.Validators.errorCheckingValueValidator;
import static com.mongodb.kafka.connect.util.VisibleForTesting.AccessModifier.PRIVATE;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;

import com.mongodb.kafka.connect.MongoSinkConnector;
import com.mongodb.kafka.connect.util.Validators;
import com.mongodb.kafka.connect.util.VisibleForTesting;

public class MongoSinkConfig extends AbstractConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkConfig.class);
  private static final String EMPTY_STRING = "";
  public static final String TOPICS_CONFIG = MongoSinkConnector.TOPICS_CONFIG;
  private static final String TOPICS_DOC =
      "A list of kafka topics for the sink connector, separated by commas";
  public static final String TOPICS_DEFAULT = EMPTY_STRING;
  private static final String TOPICS_DISPLAY = "The Kafka topics";

  public static final String TOPICS_REGEX_CONFIG = "topics.regex";
  private static final String TOPICS_REGEX_DOC =
      "Regular expression giving topics to consume. "
          + "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. "
          + "Only one of "
          + TOPICS_CONFIG
          + " or "
          + TOPICS_REGEX_CONFIG
          + " should be specified.";
  public static final String TOPICS_REGEX_DEFAULT = EMPTY_STRING;
  private static final String TOPICS_REGEX_DISPLAY = "Topics regex";

  public static final String CONNECTION_URI_CONFIG = "connection.uri";
  private static final String CONNECTION_URI_DEFAULT = "mongodb://localhost:27017";
  private static final String CONNECTION_URI_DISPLAY = "MongoDB Connection URI";
  private static final String CONNECTION_URI_DOC =
      "The connection URI as supported by the official drivers. "
          + "eg: ``mongodb://user@pass@locahost/``.";

  public static final String TOPIC_OVERRIDE_CONFIG = "topic.override.%s.%s";
  private static final String TOPIC_OVERRIDE_DEFAULT = EMPTY_STRING;
  private static final String TOPIC_OVERRIDE_DISPLAY = "Per topic configuration overrides.";
  public static final String TOPIC_OVERRIDE_DOC =
      "The overrides configuration allows for per topic customization of configuration. "
          + "The customized overrides are merged with the default configuration, to create the specific configuration for a topic.\n"
          + "For example, ``topic.override.foo.collection=bar`` will store data from the ``foo`` topic into the ``bar`` collection.\n"
          + "Note: All configuration options apart from '"
          + CONNECTION_URI_CONFIG
          + "' and '"
          + TOPICS_CONFIG
          + "' are overridable.";

  static final String PROVIDER_CONFIG = "provider";

  static final List<String> INVISIBLE_CONFIGS = singletonList(TOPIC_OVERRIDE_CONFIG);
  /**
   * Names of the <a
   * href="https://docs.mongodb.com/kafka-connector/master/sink-connector/configuration-properties/">configuration
   * properties</a> that were supported previously, but are currently ignored.
   *
   * @see #logObsoleteProperties(Collection)
   */
  @VisibleForTesting(otherwise = PRIVATE)
  static final Set<String> OBSOLETE_CONFIGS =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("max.num.retries", "retries.defer.timeout")));

  private Map<String, String> originals;
  private final Optional<List<String>> topics;
  private final Optional<Pattern> topicsRegex;
  private Map<String, MongoSinkTopicConfig> topicSinkConnectorConfigMap;
  private ConnectionString connectionString;

  public MongoSinkConfig(final Map<String, String> originals) {
    super(CONFIG, originals, false);
    this.originals = unmodifiableMap(originals);

    topics =
        getList(TOPICS_CONFIG).isEmpty()
            ? Optional.empty()
            : Optional.of(unmodifiableList(getList(TOPICS_CONFIG)));
    topicsRegex =
        getString(TOPICS_REGEX_CONFIG).isEmpty()
            ? Optional.empty()
            : Optional.of(Pattern.compile(getString(TOPICS_REGEX_CONFIG)));

    if (topics.isPresent() && topicsRegex.isPresent()) {
      throw new ConfigException(
          format(
              "%s and %s are mutually exclusive options, but both are set.",
              TOPICS_CONFIG, TOPICS_REGEX_CONFIG));
    } else if (!topics.isPresent() && !topicsRegex.isPresent()) {
      throw new ConfigException(
          format("Must configure one of %s or %s", TOPICS_CONFIG, TOPICS_REGEX_CONFIG));
    }

    connectionString = new ConnectionString(getString(CONNECTION_URI_CONFIG));
    topicSinkConnectorConfigMap =
        new ConcurrentHashMap<>(
            topics.orElse(emptyList()).stream()
                .collect(
                    Collectors.toMap((t) -> t, (t) -> new MongoSinkTopicConfig(t, originals))));

    // Process and validate overrides of regex values.
    if (topicsRegex.isPresent()) {
      Pattern topicRegex = topicsRegex.get();
      originals.keySet().stream()
          .filter(k -> k.startsWith(TOPIC_OVERRIDE_PREFIX))
          .forEach(
              k -> {
                String topic = k.substring(TOPIC_OVERRIDE_PREFIX.length()).split("\\.")[0];
                if (!topicSinkConnectorConfigMap.containsKey(topic)
                    && topicRegex.matcher(topic).matches()) {
                  topicSinkConnectorConfigMap.put(
                      topic, new MongoSinkTopicConfig(topic, originals));
                }
              });
    }
  }

  public static final ConfigDef CONFIG = createConfigDef();

  public static String createOverrideKey(final String topic, final String config) {
    if (!CONFIG.configKeys().containsKey(config)) {
      throw new ConfigException("Unknown configuration key: " + config);
    }
    return format(TOPIC_OVERRIDE_CONFIG, topic, config);
  }

  public ConnectionString getConnectionString() {
    return connectionString;
  }

  public Optional<List<String>> getTopics() {
    return topics;
  }

  public Optional<Pattern> getTopicRegex() {
    return topicsRegex;
  }

  public Map<String, String> getOriginals() {
    return originals;
  }

  public MongoSinkTopicConfig getMongoSinkTopicConfig(final String topic) {
    if (!topicSinkConnectorConfigMap.containsKey(topic)) {
      topics.ifPresent(
          topicsList -> {
            if (!topicsList.contains(topic)) {
              throw new ConfigException(
                  format("Unknown topic: %s, must be one of: %s", topic, topicsList));
            }
          });
      topicsRegex.ifPresent(
          topicRegex -> {
            if (!topicRegex.matcher(topic).matches()) {
              throw new ConfigException(
                  format("Unknown topic: %s, does not match: %s", topic, topicRegex));
            }
            if (!topicSinkConnectorConfigMap.containsKey(topic)) {
              topicSinkConnectorConfigMap.put(topic, new MongoSinkTopicConfig(topic, originals));
            }
          });
    }
    return topicSinkConnectorConfigMap.get(topic);
  }

  private static ConfigDef createConfigDef() {
    ConfigDef configDef =
        new ConfigDef() {

          @Override
          @SuppressWarnings("unchecked")
          public Map<String, ConfigValue> validateAll(final Map<String, String> props) {
            logObsoleteProperties(props.keySet());
            Map<String, ConfigValue> results = super.validateAll(props);
            INVISIBLE_CONFIGS.forEach(
                c -> {
                  if (results.containsKey(c)) {
                    results.get(c).visible(false);
                  }
                });
            // Don't validate child configs if the top level configs are broken
            if (results.values().stream().anyMatch((c) -> !c.errorMessages().isEmpty())) {
              return results;
            }

            boolean hasTopicsConfig = !props.getOrDefault(TOPICS_CONFIG, "").trim().isEmpty();
            boolean hasTopicsRegexConfig =
                !props.getOrDefault(TOPICS_REGEX_CONFIG, "").trim().isEmpty();

            if (hasTopicsConfig && hasTopicsRegexConfig) {
              results
                  .get(TOPICS_CONFIG)
                  .addErrorMessage(
                      format(
                          "%s and %s are mutually exclusive options, but both are set.",
                          TOPICS_CONFIG, TOPICS_REGEX_CONFIG));
            } else if (!hasTopicsConfig && !hasTopicsRegexConfig) {
              results
                  .get(TOPICS_CONFIG)
                  .addErrorMessage(
                      format("Must configure one of %s or %s", TOPICS_CONFIG, TOPICS_REGEX_CONFIG));
            }

            if (hasTopicsConfig) {
              List<String> topics = (List<String>) results.get(TOPICS_CONFIG).value();
              topics.forEach(
                  topic -> results.putAll(MongoSinkTopicConfig.validateAll(topic, props)));
            } else if (hasTopicsRegexConfig) {
              results.putAll(MongoSinkTopicConfig.validateRegexAll(props));
            }
            return results;
          }
        };
    String group = "Connection";
    int orderInGroup = 0;
    configDef.define(
        TOPICS_CONFIG,
        Type.LIST,
        TOPICS_DEFAULT,
        Importance.HIGH,
        TOPICS_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        TOPICS_DISPLAY);

    configDef.define(
        TOPICS_REGEX_CONFIG,
        Type.STRING,
        TOPICS_REGEX_DEFAULT,
        Validators.isAValidRegex(),
        Importance.HIGH,
        TOPICS_REGEX_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        TOPICS_REGEX_DISPLAY);

    configDef.define(
        CONNECTION_URI_CONFIG,
        Type.STRING,
        CONNECTION_URI_DEFAULT,
        errorCheckingValueValidator("A valid connection string", ConnectionString::new),
        Importance.HIGH,
        CONNECTION_URI_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        CONNECTION_URI_DISPLAY);

    addServerApiConfig(configDef);

    group = "Overrides";
    orderInGroup = 0;
    configDef.define(
        TOPIC_OVERRIDE_CONFIG,
        Type.STRING,
        TOPIC_OVERRIDE_DEFAULT,
        Validators.topicOverrideValidator(),
        Importance.LOW,
        TOPIC_OVERRIDE_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        TOPIC_OVERRIDE_DISPLAY);

    configDef.defineInternal(PROVIDER_CONFIG, Type.STRING, "", Importance.LOW);

    MongoSinkTopicConfig.BASE_CONFIG.configKeys().values().forEach(configDef::define);
    return configDef;
  }

  /**
   * This method is used in our implementation of the {@link ConfigDef#validateAll(Map)} method
   * because the original implementation silently disregards any undefined configuration properties.
   *
   * @param propertyNames Either normal configuration property names or property names prefixed with
   *     {@link MongoSinkTopicConfig#TOPIC_OVERRIDE_PREFIX} and a topic name, as specified <a
   *     href="https://docs.mongodb.com/kafka-connector/master/sink-connector/configuration-properties/topic-override/">here</a>.
   * @see #OBSOLETE_CONFIGS
   */
  static void logObsoleteProperties(final Collection<String> propertyNames) {
    propertyNames.stream()
        .filter(MongoSinkConfig::isObsolete)
        .forEach(
            obsoletePropertyName ->
                LOGGER.warn(
                    "The configuration property '{}' is obsolete"
                        + " because the sink connector started to rely on retries in the MongoDB Java driver."
                        + " Remove it as it has no effect."
                        + " If you have 'retryWrites=false' specified in the '{}' configuration property,"
                        + " then retries are disabled for the sink connector;"
                        + " remove 'retryWrites=false' from '{}' if you want to enable retries.",
                    obsoletePropertyName,
                    CONNECTION_URI_CONFIG,
                    CONNECTION_URI_CONFIG));
  }

  /** @param propertyName See {@link #logObsoleteProperties(Collection)}. */
  private static boolean isObsolete(final String propertyName) {
    String strippedPropertyName =
        propertyName.startsWith(TOPIC_OVERRIDE_PREFIX)
            ? propertyName.substring(
                propertyName.indexOf(".", TOPIC_OVERRIDE_PREFIX.length() + 1) + 1)
            : propertyName;
    return OBSOLETE_CONFIGS.contains(strippedPropertyName);
  }
}
