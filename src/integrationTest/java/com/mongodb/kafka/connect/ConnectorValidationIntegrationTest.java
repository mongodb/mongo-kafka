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

package com.mongodb.kafka.connect;

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPIC_OVERRIDE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_GRANULARITY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_METAFIELD_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_CONFIG;
import static com.mongodb.kafka.connect.util.MongoClientHelper.isAtleastFiveDotZero;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.util.ServerApiConfig;

public final class ConnectorValidationIntegrationTest {

  private static final String DEFAULT_URI = "mongodb://localhost:27017/";
  private static final String URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
  private static final String DEFAULT_DATABASE_NAME = "MongoKafkaTest";

  private static final String CUSTOM_ROLE = "customRole";
  private static final String CUSTOM_USER = "customUser";
  private static final String CUSTOM_PASSWORD = "password";
  private static final String CUSTOM_DATABASE = "customDatabase";
  private static final String CUSTOM_COLLECTION = "customCollection";
  private static MongoClient mongoClient;

  @AfterAll
  static void done() {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  @AfterEach
  void tearDown() {
    dropUserAndRoles();
    dropDatabases();
  }

  @Test
  @DisplayName("Ensure sink configuration validation works")
  void testSinkConfigValidation() {
    assertValidSink(createSinkProperties());
  }

  @Test
  @DisplayName("Ensure sink configuration validation handles invalid connections")
  void testSinkConfigValidationInvalidConnection() {
    assertInvalidSink(
        createSinkProperties("mongodb://192.0.2.0:27017/?connectTimeoutMS=1000"),
        MongoSinkConfig.CONNECTION_URI_CONFIG);
    assertInvalidSink(
        createSinkRegexProperties("mongodb://192.0.2.0:27017/?connectTimeoutMS=1000"),
        MongoSinkConfig.CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink configuration validation works with serverApi")
  void testSinkConfigValidationWithServerApi() {
    assumeTrue(isAtleastFiveDotZero(getMongoClient()));
    Map<String, String> sinkProperties = createSinkProperties();
    sinkProperties.put(ServerApiConfig.SERVER_API_VERSION_CONFIG, "1");
    assertValidSink(sinkProperties);
  }

  @Test
  @DisplayName("Ensure sink configuration validation handles invalid user")
  void testSinkConfigValidationInvalidUser() {
    assertInvalidSink(
        createSinkProperties(
            format(
                "mongodb://fakeUser:fakePass@%s/",
                String.join(",", getConnectionString().getHosts()))),
        MongoSinkConfig.CONNECTION_URI_CONFIG);
    assertInvalidSink(
        createSinkRegexProperties(
            format(
                "mongodb://fakeUser:fakePass@%s/",
                String.join(",", getConnectionString().getHosts()))),
        MongoSinkConfig.CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink configuration validation works with invalid serverApi")
  void testSinkConfigValidationWithInvalidServerApi() {
    assumeFalse(isAtleastFiveDotZero(getMongoClient()));
    Map<String, String> sinkProperties = createSinkProperties();
    sinkProperties.put(ServerApiConfig.SERVER_API_VERSION_CONFIG, "1");
    assertInvalidSink(sinkProperties, CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink validation fails with read user")
  void testSinkConfigValidationReadUser() {
    assumeTrue(isAuthEnabled());
    createUser("read");
    assertInvalidSink(
        createSinkProperties(getConnectionStringForCustomUser()),
        MongoSinkConfig.CONNECTION_URI_CONFIG);
    assertInvalidSink(
        createSinkRegexProperties(getConnectionStringForCustomUser()),
        MongoSinkConfig.CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink validation passes with readWrite user")
  void testSinkConfigValidationReadWriteUser() {
    assumeTrue(isAuthEnabled());
    createUser("readWrite");
    assertValidSink(
        createSinkProperties(getConnectionStringForCustomUser()),
        MongoSinkConfig.CONNECTION_URI_CONFIG);
    assertValidSink(
        createSinkRegexProperties(getConnectionStringForCustomUser()),
        MongoSinkConfig.CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink validation passes with readWrite user on specific db")
  void testSinkConfigValidationReadWriteOnSpecificDatabase() {
    assumeTrue(isAuthEnabled());
    createUserFromDocument(format("{ role: 'readWrite', db: '%s'}", CUSTOM_DATABASE));

    Map<String, String> properties = createSinkProperties(getConnectionStringForCustomUser());

    // Different database than has permissions for
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
    assertValidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Regex tests
    properties = createSinkRegexProperties(getConnectionStringForCustomUser());

    // Different database than has permissions for
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
    assertValidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink validation passes with specific collection based privileges")
  void testSinkConfigValidationCollectionBasedPrivileges() {
    assumeTrue(isAuthEnabled());
    createUserWithCustomRole(
        asList(
            format(
                "{resource: {db: '%s', collection: '%s'}, actions: ['find', 'insert'] }",
                CUSTOM_DATABASE, CUSTOM_COLLECTION),
            "{resource: { cluster : true }, actions: ['remove', 'update'] }"));

    Map<String, String> properties = createSinkProperties(getConnectionStringForCustomUser());

    // Different database than has permissions for
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Different collection than has permissions for
    properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Same collection than has permissions for
    properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, CUSTOM_COLLECTION);
    assertValidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Regex tests
    properties = createSinkRegexProperties(getConnectionStringForCustomUser());

    // Different database than has permissions for
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Different collection than has permissions for
    properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Same collection than has permissions for
    properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, CUSTOM_COLLECTION);
    assertValidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink timeseries validation works as expected")
  void testSinkConfigValidationTimeseries() {
    assumeTrue(isAtleastFiveDotZero(getMongoClient()));

    // Missing timefield
    Map<String, String> properties = createSinkProperties();
    properties.put(TIMESERIES_GRANULARITY_CONFIG, "hours");
    assertInvalidSink(properties, TIMESERIES_GRANULARITY_CONFIG);

    properties.put(TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG, "1");
    assertInvalidSink(properties, TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG);

    properties.put(TIMESERIES_METAFIELD_CONFIG, "meta");
    assertInvalidSink(properties, TIMESERIES_METAFIELD_CONFIG);

    properties.put(TIMESERIES_TIMEFIELD_CONFIG, "ts");
    assertValidSink(properties);

    // Confirm collection created
    assertTrue(collectionExists());

    // Create normal collection confirm invalid.
    dropDatabases();
    getMongoClient().getDatabase(DEFAULT_DATABASE_NAME).createCollection("test");
    assertInvalidSink(properties, TIMESERIES_TIMEFIELD_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink timeseries validation works as expected when using regex config")
  void testSinkConfigValidationTimeseriesRegex() {
    assumeTrue(isAtleastFiveDotZero(getMongoClient()));

    // Missing timefield
    Map<String, String> properties = createSinkRegexProperties();
    properties.put(TIMESERIES_GRANULARITY_CONFIG, "hours");
    assertInvalidSink(properties);
    assertInvalidSink(properties, TIMESERIES_GRANULARITY_CONFIG);

    properties.put(TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG, "1");
    assertInvalidSink(properties, TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG);

    properties.put(TIMESERIES_METAFIELD_CONFIG, "meta");
    assertInvalidSink(properties, TIMESERIES_METAFIELD_CONFIG);

    properties.put(TIMESERIES_TIMEFIELD_CONFIG, "ts");
    assertValidSink(properties);

    // Confirm no collection created
    assertFalse(collectionExists());
  }

  @Test
  @DisplayName(
      "Ensure sink timeseries validation works as expected when using regex config with overrides")
  void testSinkConfigValidationTimeseriesRegexWithOverrides() {
    assumeTrue(isAtleastFiveDotZero(getMongoClient()));

    Map<String, String> properties = createSinkRegexProperties();
    properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, "test");
    properties.put(
        format(TOPIC_OVERRIDE_CONFIG, "topic-test", TIMESERIES_GRANULARITY_CONFIG), "hours");
    assertInvalidSink(properties, TIMESERIES_GRANULARITY_CONFIG);

    properties.put(
        format(TOPIC_OVERRIDE_CONFIG, "topic-test", TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG), "1");
    assertInvalidSink(properties, TIMESERIES_EXPIRE_AFTER_SECONDS_CONFIG);

    properties.put(
        format(TOPIC_OVERRIDE_CONFIG, "topic-test", TIMESERIES_METAFIELD_CONFIG), "meta");
    assertInvalidSink(properties, TIMESERIES_METAFIELD_CONFIG);

    properties.put(format(TOPIC_OVERRIDE_CONFIG, "topic-test", TIMESERIES_TIMEFIELD_CONFIG), "ts");
    assertValidSink(properties);

    // Confirm collection created thanks to override name
    assertTrue(collectionExists());
  }

  @Test
  @DisplayName("Ensure sink validation when timeseries not supported")
  void testSinkConfigValidationTimeseriesNotSupported() {
    assumeFalse(isAtleastFiveDotZero(getMongoClient()));

    Map<String, String> properties = createSinkProperties();
    properties.put(TIMESERIES_TIMEFIELD_CONFIG, "ts");
    assertInvalidSink(properties, TIMESERIES_TIMEFIELD_CONFIG);
  }

  @Test
  @DisplayName("Ensure sink validation timeseries auth permissions")
  void testSinkConfigAuthValidationTimeseries() {
    assumeTrue(isAuthEnabled());
    assumeTrue(isAtleastFiveDotZero(getMongoClient()));

    Map<String, String> properties = createSinkProperties(getConnectionStringForCustomUser());
    properties.put(MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_CONFIG, "ts");

    // Missing permissions
    createUserFromDocument(format("{ role: 'read', db: '%s'}", getDatabaseName()));
    assertInvalidSink(properties);

    // Add permissions
    dropUserAndRoles();
    createUserFromDocument(format("{ role: 'readWrite', db: '%s'}", getDatabaseName()));

    assertValidSink(properties);
    assertTrue(collectionExists());
  }

  @Test
  @DisplayName(
      "Ensure sink validation passes with specific collection based privileges with a different auth db")
  void testSinkConfigValidationCollectionBasedDifferentAuthPrivileges() {
    assumeTrue(isAuthEnabled());
    createUserWithCustomRole(
        CUSTOM_DATABASE,
        singletonList(
            format("{resource: {db: '%s', collection: '%s'}, ", CUSTOM_DATABASE, CUSTOM_COLLECTION)
                + "actions: ['find', 'insert', 'remove', 'update'] }"),
        emptyList());

    Map<String, String> properties =
        createSinkProperties(getConnectionStringForCustomUser(CUSTOM_DATABASE));

    // Different database than has permissions for
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Different collection than has permissions for
    properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Same collection than has permissions for
    properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, CUSTOM_COLLECTION);
    assertValidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Regex tests
    properties = createSinkRegexProperties(getConnectionStringForCustomUser(CUSTOM_DATABASE));

    // Different database than has permissions for
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Different collection than has permissions for
    properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
    assertInvalidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);

    // Same collection than has permissions for
    properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, CUSTOM_COLLECTION);
    assertValidSink(properties, MongoSinkConfig.CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure source configuration validation works")
  void testSourceConfigValidation() {
    assertValidSource(createSourceProperties());
  }

  @Test
  @DisplayName("Ensure source configuration validation works with serverApi")
  void testSourceConfigValidationWithValidServerApi() {
    assumeTrue(isAtleastFiveDotZero(getMongoClient()));
    Map<String, String> sourceProperties = createSourceProperties();
    sourceProperties.put(ServerApiConfig.SERVER_API_VERSION_CONFIG, "1");
    assertValidSource(sourceProperties);
  }

  @Test
  @DisplayName("Ensure source configuration validation handles invalid connections")
  void testSourceConfigValidationInvalidConnection() {
    assertInvalidSource(createSourceProperties("mongodb://192.0.2.0:27017/?connectTimeoutMS=1000"));
  }

  @Test
  @DisplayName("Ensure source configuration validation works with invalid serverApi")
  void testSourceConfigValidationWithInvalidServerApi() {
    assumeFalse(isAtleastFiveDotZero(getMongoClient()));
    Map<String, String> sourceProperties = createSourceProperties();
    sourceProperties.put(ServerApiConfig.SERVER_API_VERSION_CONFIG, "1");
    assertInvalidSource(sourceProperties, CONNECTION_URI_CONFIG);
  }

  @Test
  @DisplayName("Ensure source configuration validation handles invalid user")
  void testSourceConfigValidationInvalidUser() {
    assertInvalidSource(
        createSourceProperties(
            format(
                "mongodb://fakeUser:fakePass@%s/",
                String.join(",", getConnectionString().getHosts()))));
  }

  @Test
  @DisplayName("Ensure source validation passes with read user")
  void testSourceConfigValidationReadUser() {
    assumeTrue(isAuthEnabled());
    createUser("read");
    assertValidSource(createSourceProperties(getConnectionStringForCustomUser()));
  }

  @Test
  @DisplayName("Ensure source validation passes with read user on specific db")
  void testSourceConfigValidationReadUserOnSpecificDatabase() {
    assumeTrue(isAuthEnabled());
    createUserFromDocument(format("{ role: 'read', db: '%s' }", CUSTOM_DATABASE));

    Map<String, String> properties = createSourceProperties(getConnectionStringForCustomUser());

    // Different database than has permissions for
    assertInvalidSource(properties);

    properties.put(MongoSourceConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
    assertValidSource(properties);
  }

  @Test
  @DisplayName("Ensure source validation passes with specific collection based privileges")
  void testSourceConfigValidationCollectionBasedPrivileges() {
    assumeTrue(isAuthEnabled());
    createUserWithCustomRole(
        asList(
            format(
                "{resource: {db: '%s', collection: '%s'}, actions: ['find', 'insert'] }",
                CUSTOM_DATABASE, CUSTOM_COLLECTION),
            "{resource: { cluster : true }, actions: ['changeStream'] }"));

    Map<String, String> properties = createSourceProperties(getConnectionStringForCustomUser());

    // Different database than has permissions for
    assertInvalidSource(properties);

    // Different collection than has permissions for
    properties.put(MongoSourceConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
    assertInvalidSource(properties);

    // Same collection than has permissions for
    properties.put(MongoSourceConfig.COLLECTION_CONFIG, CUSTOM_COLLECTION);
    assertValidSource(properties);
  }

  // Helper methods
  private void assertInvalidSource(final Map<String, String> properties) {
    assertFalse(getSourceErrors(properties).isEmpty(), "Source had valid configuration");
  }

  private void assertInvalidSource(final Map<String, String> properties, final String configName) {
    Optional<ConfigValue> configValue =
        getSourceErrors(properties).stream().filter(cv -> cv.name().equals(configName)).findFirst();
    assertTrue(
        configValue.isPresent(),
        format(
            "No error for '%s': %s\nErrors: %s\nProperties: %s",
            configName,
            properties.getOrDefault(configName, ""),
            getSourceErrors(properties),
            properties));
    assertFalse(
        configValue.get().errorMessages().isEmpty(), format("No error for '%s'", configName));
  }

  private void assertValidSource(final Map<String, String> properties) {
    assumeTrue(isReplicaSetOrSharded());
    List<ConfigValue> sourceErrors = getSourceErrors(properties);
    assertTrue(
        sourceErrors.isEmpty(),
        "Sink had invalid configuration: "
            + sourceErrors.stream()
                .map(cv -> format("'%s': %s", cv.name(), cv.errorMessages()))
                .collect(Collectors.toList()));
  }

  private List<ConfigValue> getSourceErrors(final Map<String, String> properties) {
    Config config = new MongoSourceConnector().validate(properties);
    return config.configValues().stream()
        .filter(cv -> !cv.errorMessages().isEmpty())
        .collect(Collectors.toList());
  }

  private void assertInvalidSink(final Map<String, String> properties, final String configName) {
    Optional<ConfigValue> configValue =
        getSinkErrors(properties).stream().filter(cv -> cv.name().equals(configName)).findFirst();
    assertTrue(
        configValue.isPresent(),
        format(
            "No error for '%s': %s\nErrors: %s\nProperties: %s",
            configName,
            properties.getOrDefault(configName, ""),
            getSinkErrors(properties),
            properties));
    assertFalse(
        configValue.get().errorMessages().isEmpty(), format("No error for '%s'", configName));
  }

  private void assertInvalidSink(final Map<String, String> properties) {
    assertFalse(getSinkErrors(properties).isEmpty(), "Sink had valid configuration");
  }

  private void assertValidSink(final Map<String, String> properties, final String configName) {
    Optional<ConfigValue> configValue =
        getSinkErrors(properties).stream().filter(cv -> cv.name().equals(configName)).findFirst();
    configValue.ifPresent(
        (cv) ->
            assertTrue(
                cv.errorMessages().isEmpty(),
                format("%s invalid: %s", cv.name(), cv.errorMessages())));
  }

  private void assertValidSink(final Map<String, String> properties) {
    List<ConfigValue> sinkErrors = getSinkErrors(properties);
    assertTrue(
        sinkErrors.isEmpty(),
        "Sink had invalid configuration: "
            + sinkErrors.stream()
                .map(cv -> format("'%s': %s", cv.name(), cv.errorMessages()))
                .collect(Collectors.toList()));
  }

  private List<ConfigValue> getSinkErrors(final Map<String, String> properties) {
    Config config = new MongoSinkConnector().validate(properties);
    return config.configValues().stream()
        .filter(cv -> !cv.errorMessages().isEmpty())
        .collect(Collectors.toList());
  }

  private boolean collectionExists() {
    return collectionExists(DEFAULT_DATABASE_NAME, "test");
  }

  private boolean collectionExists(final String databaseName, final String collectionName) {
    return getMongoClient()
        .getDatabase(databaseName)
        .listCollectionNames()
        .into(new ArrayList<>())
        .contains(collectionName);
  }

  private void createUser(final String role) {
    createUser(getAuthSource(), role);
  }

  private void createUser(final String databaseName, final String role) {
    createUser(databaseName, singletonList(role));
  }

  private void createUser(final String databaseName, final List<String> roles) {
    String userRoles = roles.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
    getMongoClient()
        .getDatabase(databaseName)
        .runCommand(
            Document.parse(
                format(
                    "{createUser: '%s', pwd: '%s', roles: [%s]}",
                    CUSTOM_USER, CUSTOM_PASSWORD, userRoles)));
  }

  private void createUserFromDocument(final String role) {
    createUserFromDocument(singletonList(role));
  }

  private void createUserFromDocument(final List<String> roles) {
    getMongoClient()
        .getDatabase(getAuthSource())
        .runCommand(
            Document.parse(
                format(
                    "{createUser: '%s', pwd: '%s', roles: [%s]}",
                    CUSTOM_USER, CUSTOM_PASSWORD, String.join(",", roles))));
  }

  private void createUserWithCustomRole(final List<String> privileges) {
    createUserWithCustomRole(privileges, emptyList());
  }

  private void createUserWithCustomRole(final List<String> privileges, final List<String> roles) {
    createUserWithCustomRole(getAuthSource(), privileges, roles);
  }

  private void createUserWithCustomRole(
      final String databaseName, final List<String> privileges, final List<String> roles) {
    getMongoClient()
        .getDatabase(databaseName)
        .runCommand(
            Document.parse(
                format(
                    "{createRole: '%s', privileges: [%s], roles: [%s]}",
                    CUSTOM_ROLE, String.join(",", privileges), String.join(",", roles))));
    createUser(databaseName, CUSTOM_ROLE);
  }

  private void dropUserAndRoles() {
    if (isAuthEnabled()) {
      List<MongoDatabase> databases =
          asList(
              getMongoClient().getDatabase(getAuthSource()),
              getMongoClient().getDatabase(CUSTOM_DATABASE));

      for (final MongoDatabase database : databases) {
        tryAndIgnore(
            () -> database.runCommand(Document.parse(format("{dropUser: '%s'}", CUSTOM_USER))));
        tryAndIgnore(
            () -> database.runCommand(Document.parse(format("{dropRole: '%s'}", CUSTOM_ROLE))));
        tryAndIgnore(() -> database.runCommand(Document.parse("{invalidateUserCache: 1}")));
      }
    }
  }

  private void dropDatabases() {
    tryAndIgnore(() -> getMongoClient().getDatabase(DEFAULT_DATABASE_NAME).drop());
    tryAndIgnore(() -> getMongoClient().getDatabase(CUSTOM_DATABASE).drop());
  }

  public static void tryAndIgnore(final Runnable r) {
    try {
      r.run();
    } catch (Exception e) {
      // Ignore
    }
  }

  private MongoClient getMongoClient() {
    if (mongoClient == null) {
      mongoClient = MongoClients.create(getConnectionString());
    }
    return mongoClient;
  }

  private String getConnectionStringForCustomUser() {
    return getConnectionStringForCustomUser(getAuthSource());
  }

  private String getConnectionStringForCustomUser(final String authSource) {
    String connectionString = getConnectionString().toString();
    String scheme = getConnectionString().isSrvProtocol() ? "mongodb+srv://" : "mongodb://";
    String hostsAndQuery = connectionString.split("@")[1];
    String userConnectionString =
        format("%s%s:%s@%s", scheme, CUSTOM_USER, CUSTOM_PASSWORD, hostsAndQuery);
    userConnectionString =
        userConnectionString.replace(
            format("authSource=%s", getAuthSource()), format("authSource=%s", authSource));

    if (!userConnectionString.contains("authSource")) {
      String separator = userConnectionString.contains("/?") ? "&" : "?";
      userConnectionString =
          format("%s%sauthSource=%s", userConnectionString, separator, authSource);
    }
    return userConnectionString;
  }

  private boolean isAuthEnabled() {
    return getConnectionString().getCredential() != null;
  }

  private String getAuthSource() {
    return Optional.ofNullable(getConnectionString().getCredential())
        .map(MongoCredential::getSource)
        .orElseThrow(() -> new AssertionError("No auth credential"));
  }

  private boolean isReplicaSetOrSharded() {
    try (MongoClient mongoClient = MongoClients.create(getConnectionString())) {
      Document isMaster =
          mongoClient.getDatabase("admin").runCommand(BsonDocument.parse("{isMaster: 1}"));
      return isMaster.containsKey("setName") || isMaster.get("msg", "").equals("isdbgrid");
    } catch (Exception e) {
      return false;
    }
  }

  private String getDatabaseName() {
    String databaseName = getConnectionString().getDatabase();
    return databaseName != null ? databaseName : DEFAULT_DATABASE_NAME;
  }

  private ConnectionString getConnectionString() {
    String mongoURIProperty = System.getProperty(URI_SYSTEM_PROPERTY_NAME);
    String mongoURIString =
        mongoURIProperty == null || mongoURIProperty.isEmpty() ? DEFAULT_URI : mongoURIProperty;
    return new ConnectionString(mongoURIString);
  }

  private Map<String, String> createSinkProperties() {
    return createSinkProperties(getConnectionString().toString());
  }

  private Map<String, String> createSinkProperties(final String connectionString) {
    Map<String, String> properties = createProperties(connectionString);
    properties.put(MongoSinkConfig.TOPICS_CONFIG, "test");
    properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, DEFAULT_DATABASE_NAME);
    properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, "test");
    return properties;
  }

  private Map<String, String> createSinkRegexProperties() {
    return createSinkRegexProperties(getConnectionString().toString());
  }

  private Map<String, String> createSinkRegexProperties(final String connectionString) {
    Map<String, String> properties = createSinkProperties(connectionString);
    properties.remove(MongoSinkConfig.TOPICS_CONFIG);
    properties.remove(MongoSinkTopicConfig.COLLECTION_CONFIG, "test");
    properties.put(MongoSinkConfig.TOPICS_REGEX_CONFIG, "topic-(.*)");
    return properties;
  }

  private Map<String, String> createSourceProperties() {
    return createSourceProperties(getConnectionString().toString());
  }

  private Map<String, String> createSourceProperties(final String connectionString) {
    return createProperties(connectionString);
  }

  private Map<String, String> createProperties(final String connectionString) {
    Map<String, String> properties = new HashMap<>();
    properties.put(MongoSinkConfig.CONNECTION_URI_CONFIG, connectionString);
    properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, getDatabaseName());
    return properties;
  }
}
