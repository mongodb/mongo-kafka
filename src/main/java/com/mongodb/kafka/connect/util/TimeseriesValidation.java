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
package com.mongodb.kafka.connect.util;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_GRANULARITY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_METAFIELD_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getConfigByName;
import static java.lang.String.format;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.namespace.mapping.DefaultNamespaceMapper;
import com.mongodb.kafka.connect.sink.namespace.mapping.NamespaceMapper;

public final class TimeseriesValidation {

  private static final String COLLSTATS = "collStats";
  private static final String TIMESERIES = "timeseries";
  private static final String TOPIC_OVERRIDE_PREFIX = "topic.override.";

  public static void validateConfigAndCollection(
      final MongoClient mongoClient, final MongoSinkTopicConfig topicConfig, final Config config) {

    validateConfigAndTimeseriesSupport(mongoClient, topicConfig, config);
    if (!topicConfig.isTimeseries()
        || config.configValues().stream().anyMatch(cv -> !cv.errorMessages().isEmpty())) {
      return;
    }

    NamespaceMapper namespaceMapper = topicConfig.getNamespaceMapper();
    if (namespaceMapper instanceof DefaultNamespaceMapper) {
      getConfigByName(config, TIMESERIES_TIMEFIELD_CONFIG)
          .ifPresent(
              configValue -> {
                try {
                  validateCollection(
                      mongoClient, namespaceMapper.getNamespace(null, null), topicConfig);
                } catch (Exception e) {
                  configValue.addErrorMessage(e.getMessage());
                }
              });
    }
  }

  public static void validTopicRegexConfigAndCollection(
      final MongoClient mongoClient, final MongoSinkConfig sinkConfig, final Config config) {
    assert sinkConfig.getTopicRegex().isPresent();
    Pattern pattern = sinkConfig.getTopicRegex().get();

    Set<String> topicsWithOverrides =
        sinkConfig.getOriginals().keySet().stream()
            .filter(k -> k.startsWith(TOPIC_OVERRIDE_PREFIX))
            .map(k -> k.substring(TOPIC_OVERRIDE_PREFIX.length()))
            .map(
                k -> {
                  int index = k.indexOf(".");
                  return k.substring(0, index > 0 ? index : k.length());
                })
            .filter(k -> k.matches(pattern.pattern()))
            .collect(Collectors.toSet());

    if (!topicsWithOverrides.isEmpty()) {
      topicsWithOverrides.forEach(
          t ->
              validateConfigAndCollection(
                  mongoClient,
                  new MongoSinkTopicConfig(t, sinkConfig.getOriginals(), false),
                  config));
    } else {
      validateConfigAndCollection(
          mongoClient,
          new MongoSinkTopicConfig("__default", sinkConfig.getOriginals(), false),
          config);
    }
  }

  public static void validateCollection(
      final MongoClient mongoClient,
      final MongoNamespace namespace,
      final MongoSinkTopicConfig config) {

    try {

      Document collStats = getCollStats(mongoClient, namespace);
      if (collStats.containsKey(TIMESERIES)) {
        return;
      }
      if (collStats.getInteger("nindexes") > 0) {
        throw new ConfigException(
            format(
                "A collection already exists for: `%s` that is not a timeseries collection.",
                namespace.getFullName()));
      }
      createCollection(mongoClient, namespace, createCollectionOptions(config));
    } catch (MongoException e) {
      // Check duplicate collection
      if (e.getCode() == 48) {
        if (!isTimeseriesCollection(mongoClient, namespace)) {
          throw new ConfigException(
              format(
                  "A collection already exists for: `%s` that is not a timeseries collection.",
                  namespace.getFullName()),
              e);
        }
      } else if (e.getCode() == 13) {
        throw new ConfigException(
            format(
                "Failed to create collection for: `%s`. "
                    + "Unauthorized, user does not have the correct permissions to create the collection. %s",
                namespace.getFullName(), e.getMessage()),
            e);
      }
      throw new ConfigException(
          format(
              "Failed to create collection for: `%s`. %s",
              namespace.getFullName(), e.getMessage()));
    }
  }

  private static void validateConfigAndTimeseriesSupport(
      final MongoClient mongoClient, final MongoSinkTopicConfig topicConfig, final Config config) {
    if (!topicConfig.isTimeseries()) {
      String metaField = topicConfig.getString(TIMESERIES_METAFIELD_CONFIG);
      if (!metaField.equals(MongoSinkTopicConfig.TIMESERIES_METAFIELD_DEFAULT)) {
        getConfigByName(config, TIMESERIES_METAFIELD_CONFIG)
            .ifPresent(
                configValue ->
                    configValue.addErrorMessage(
                        format(
                            "Missing timeseries configuration: `%s`",
                            TIMESERIES_TIMEFIELD_CONFIG)));
      }

      long expireAfter = topicConfig.getLong(TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG);
      if (expireAfter != MongoSinkTopicConfig.TIMESERIES_EXPIRE_AFTER_SECONDS_DEFAULT) {
        getConfigByName(config, TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG)
            .ifPresent(
                configValue ->
                    configValue.addErrorMessage(
                        format(
                            "Missing timeseries configuration: `%s`",
                            TIMESERIES_TIMEFIELD_CONFIG)));
      }

      String granularity =
          topicConfig.getString(TIMESERIES_GRANULARITY_CONFIG).toLowerCase(Locale.ROOT);
      if (!granularity.equals(MongoSinkTopicConfig.TIMESERIES_GRANULARITY_DEFAULT.value())) {
        getConfigByName(config, TIMESERIES_GRANULARITY_CONFIG)
            .ifPresent(
                configValue ->
                    configValue.addErrorMessage(
                        format(
                            "Missing timeseries configuration: `%s`",
                            TIMESERIES_TIMEFIELD_CONFIG)));
      }
    } else if (!isAtleastFiveDotZero(mongoClient)) {
      getConfigByName(config, TIMESERIES_TIMEFIELD_CONFIG)
          .ifPresent(
              configValue ->
                  configValue.addErrorMessage("Timeseries support requires MongoDB 5.0 or newer"));
    }
  }

  private static CreateCollectionOptions createCollectionOptions(
      final MongoSinkTopicConfig config) {
    assert config.isTimeseries();

    TimeSeriesOptions timeSeriesOptions =
        new TimeSeriesOptions(config.getString(TIMESERIES_TIMEFIELD_CONFIG).trim());

    String metaField = config.getString(TIMESERIES_METAFIELD_CONFIG).trim();
    if (!metaField.isEmpty()) {
      timeSeriesOptions.metaField(metaField);
    }

    MongoSinkTopicConfig.TimeSeriesGranularity granularity =
        MongoSinkTopicConfig.TimeSeriesGranularity.valueOf(
            config.getString(TIMESERIES_GRANULARITY_CONFIG).trim().toUpperCase());
    switch (granularity) {
      case SECONDS:
        timeSeriesOptions.granularity(TimeSeriesGranularity.SECONDS);
        break;
      case MINUTES:
        timeSeriesOptions.granularity(TimeSeriesGranularity.MINUTES);
        break;
      case HOURS:
        timeSeriesOptions.granularity(TimeSeriesGranularity.HOURS);
        break;
      default:
        // Do nothing
    }

    CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions();
    createCollectionOptions.timeSeriesOptions(timeSeriesOptions);

    Long expireAfterSeconds = config.getLong(TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG);
    if (expireAfterSeconds > 0) {
      createCollectionOptions.expireAfter(expireAfterSeconds, TimeUnit.SECONDS);
    }
    return createCollectionOptions;
  }

  private static void createCollection(
      final MongoClient mongoClient,
      final MongoNamespace namespace,
      final CreateCollectionOptions options) {
    mongoClient
        .getDatabase(namespace.getDatabaseName())
        .createCollection(namespace.getCollectionName(), options);
  }

  private static Document getCollStats(
      final MongoClient mongoClient, final MongoNamespace namespace) {
    return mongoClient
        .getDatabase(namespace.getDatabaseName())
        .runCommand(new Document(COLLSTATS, namespace.getCollectionName()));
  }

  private static boolean isTimeseriesCollection(
      final MongoClient mongoClient, final MongoNamespace namespace) {
    return getCollStats(mongoClient, namespace).containsKey(TIMESERIES);
  }

  private static boolean isAtleastFiveDotZero(final MongoClient mongoClient) {
    try {
      return mongoClient
              .getDatabase("admin")
              .runCommand(BsonDocument.parse("{hello: 1}"))
              .get("maxWireVersion", 0)
          >= 13;
    } catch (Exception e) {
      return false;
    }
  }

  private TimeseriesValidation() {}
}
