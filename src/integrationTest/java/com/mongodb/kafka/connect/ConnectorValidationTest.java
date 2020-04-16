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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig;

public final class ConnectorValidationTest {

    private static final String DEFAULT_URI = "mongodb://localhost:27017";
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
    }

    @Test
    @DisplayName("Ensure sink configuration validation works")
    void testSinkConfigValidation() {
        assertValidSink(createSinkProperties());
    }

    @Test
    @DisplayName("Ensure sink configuration validation handles invalid connections")
    void testSinkConfigValidationInvalidConnection() {
        assertInvalidSink(createSinkProperties("mongodb://192.0.2.0:27017/?connectTimeoutMS=1000"));
    }

    @Test
    @DisplayName("Ensure sink configuration validation handles invalid user")
    void testSinkConfigValidationInvalidUser() {
        assertInvalidSink(createSinkProperties(format("mongodb://fakeUser:fakePass@%s/",
                        String.join(",", getConnectionString().getHosts()))));
    }

    @Test
    @DisplayName("Ensure sink validation fails with read user")
    void testSinkConfigValidationReadUser() {
        assumeTrue(isAuthEnabled());
        createUser("read");
        assertInvalidSink(createSinkProperties(getConnectionStringForCustomUser()));
    }

    @Test
    @DisplayName("Ensure sink validation passes with readWrite user")
    void testSinkConfigValidationReadWriteUser() {
        assumeTrue(isAuthEnabled());
        createUser("readWrite");
        assertValidSink(createSinkProperties(getConnectionStringForCustomUser()));
    }

    @Test
    @DisplayName("Ensure sink validation passes with readWrite user on specific db")
    void testSinkConfigValidationReadWriteOnSpecificDatabase() {
        assumeTrue(isAuthEnabled());
        createUserFromDocument(format("{ role: 'readWrite', db: '%s'}", CUSTOM_DATABASE));

        Map<String, String> properties = createSinkProperties(getConnectionStringForCustomUser());

        // Different database than has permissions for
        assertInvalidSink(properties);

        properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
        assertValidSink(properties);
    }

    @Test
    @DisplayName("Ensure sink validation passes with specific collection based privileges")
    void testSinkConfigValidationCollectionBasedPrivileges() {
        assumeTrue(isAuthEnabled());
        createUserWithCustomRole(asList(
                format("{resource: {db: '%s', collection: '%s'}, actions: ['find', 'insert'] }", CUSTOM_DATABASE, CUSTOM_COLLECTION),
                "{resource: { cluster : true }, actions: ['remove', 'update'] }"));

        Map<String, String> properties = createSinkProperties(getConnectionStringForCustomUser());

        // Different database than has permissions for
        assertInvalidSink(properties);

        // Different collection than has permissions for
        properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
        assertInvalidSink(properties);

        // Different collection than has permissions for
        properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, CUSTOM_COLLECTION);
        assertValidSink(properties);
    }

    @Test
    @DisplayName("Ensure sink validation passes with specific collection based privileges with a different auth db")
    void testSinkConfigValidationCollectionBasedDifferentAuthPrivileges() {
        assumeTrue(isAuthEnabled());
        createUserWithCustomRole(CUSTOM_DATABASE,
                singletonList(
                        format("{resource: {db: '%s', collection: '%s'}, ", CUSTOM_DATABASE, CUSTOM_COLLECTION)
                                + "actions: ['find', 'insert', 'remove', 'update'] }"), emptyList());

        Map<String, String> properties = createSinkProperties(getConnectionStringForCustomUser(CUSTOM_DATABASE));

        // Different database than has permissions for
        assertInvalidSink(properties);

        // Different collection than has permissions for
        properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, CUSTOM_DATABASE);
        assertInvalidSink(properties);

        // Same collection than has permissions for
        properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, CUSTOM_COLLECTION);
        assertValidSink(properties);
    }

    @Test
    @DisplayName("Ensure source configuration validation works")
    void testSourceConfigValidation() {
        assertValidSource(createSourceProperties());
    }

    @Test
    @DisplayName("Ensure source configuration validation handles invalid connections")
    void testSourceConfigValidationInvalidConnection() {
        assertInvalidSource(createSourceProperties("mongodb://192.0.2.0:27017/?connectTimeoutMS=1000"));
    }

    @Test
    @DisplayName("Ensure source configuration validation handles invalid user")
    void testSourceConfigValidationInvalidUser() {
        assertInvalidSource(createSourceProperties(format("mongodb://fakeUser:fakePass@%s/",
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
        createUserWithCustomRole(asList(
                format("{resource: {db: '%s', collection: '%s'}, actions: ['find', 'insert'] }", CUSTOM_DATABASE, CUSTOM_COLLECTION),
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
        Config config = new MongoSourceConnector().validate(properties);
        List<String> errorMessages = getConfigValue(config, MongoSourceConfig.CONNECTION_URI_CONFIG).errorMessages();
        assertFalse(errorMessages.isEmpty(), "ErrorMessages shouldn't be empty");
    }

    private void assertValidSource(final Map<String, String> properties) {
        assumeTrue(isReplicaSetOrSharded());
        Config config = new MongoSourceConnector().validate(properties);
        List<String> errorMessages = getConfigValue(config, MongoSourceConfig.CONNECTION_URI_CONFIG).errorMessages();
        assertTrue(errorMessages.isEmpty(), format("ErrorMessages not empty: %s", errorMessages));
    }

    private void assertInvalidSink(final Map<String, String> properties) {
        Config config = new MongoSinkConnector().validate(properties);
        List<String> errorMessages = getConfigValue(config, MongoSourceConfig.CONNECTION_URI_CONFIG).errorMessages();
        assertFalse(errorMessages.isEmpty(), "ErrorMessages shouldn't be empty");
    }

    private void assertValidSink(final Map<String, String> properties) {
        Config config = new MongoSinkConnector().validate(properties);
        List<String> errorMessages = getConfigValue(config, MongoSourceConfig.CONNECTION_URI_CONFIG).errorMessages();
        assertTrue(errorMessages.isEmpty(), format("ErrorMessages not empty: %s", errorMessages));
    }

    private void createUser(final String role) {
        createUser(getConnectionString().getCredential().getSource(), role);
    }

    private void createUser(final String databaseName, final String role) {
        createUser(databaseName, singletonList(role));
    }

    private void createUser(final String databaseName, final List<String> roles) {
        String userRoles = roles.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
        getMongoClient().getDatabase(databaseName)
                .runCommand(Document.parse(format("{createUser: '%s', pwd: '%s', roles: [%s]}", CUSTOM_USER, CUSTOM_PASSWORD, userRoles)));
    }

    private void createUserFromDocument(final String role) {
        createUserFromDocument(singletonList(role));
    }

    private void createUserFromDocument(final List<String> roles) {
        getMongoClient().getDatabase(getConnectionString().getCredential().getSource())
                .runCommand(Document.parse(format("{createUser: '%s', pwd: '%s', roles: [%s]}", CUSTOM_USER, CUSTOM_PASSWORD,
                        String.join(",", roles))));
    }

    private void createUserWithCustomRole(final List<String> privileges) {
        createUserWithCustomRole(privileges, emptyList());
    }

    private void createUserWithCustomRole(final List<String> privileges, final List<String> roles) {
        createUserWithCustomRole(getConnectionString().getCredential().getSource(), privileges, roles);
    }

    private void createUserWithCustomRole(final String databaseName, final List<String> privileges, final List<String> roles) {
        getMongoClient().getDatabase(databaseName)
                .runCommand(Document.parse(format("{createRole: '%s', privileges: [%s], roles: [%s]}", CUSTOM_ROLE,
                        String.join(",", privileges), String.join(",", roles))));
        createUser(databaseName, CUSTOM_ROLE);
    }

    private void dropUserAndRoles() {
        if (isAuthEnabled()) {
            List<MongoDatabase> databases = asList(
                    getMongoClient().getDatabase(getConnectionString().getCredential().getSource()),
                    getMongoClient().getDatabase(CUSTOM_DATABASE));

            for (final MongoDatabase database : databases) {
                tryAndIgnore(() -> database.runCommand(Document.parse(format("{dropUser: '%s'}", CUSTOM_USER))));
                tryAndIgnore(() -> database.runCommand(Document.parse(format("{dropRole: '%s'}", CUSTOM_ROLE))));
                tryAndIgnore(() -> database.runCommand(Document.parse("{invalidateUserCache: 1}")));
            }
        }
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
        return getConnectionStringForCustomUser(getConnectionString().getCredential().getSource());
    }

    private String getConnectionStringForCustomUser(final String authSource) {
        String connectionString = getConnectionString().toString();
        String scheme = getConnectionString().isSrvProtocol() ? "mongodb+srv://" : "mongodb://";
        String hostsAndQuery = connectionString.split("@")[1];
        String userConnectionString = format("%s%s:%s@%s", scheme, CUSTOM_USER, CUSTOM_PASSWORD, hostsAndQuery);
        userConnectionString = userConnectionString.replace(format("authSource=%s", getConnectionString().getCredential().getSource()),
                    format("authSource=%s", authSource));

        if (!userConnectionString.contains("authSource")) {
            String separator = userConnectionString.contains("/?") ? "&" : "?";
            userConnectionString = format("%s%sauthSource=%s", userConnectionString, separator, authSource);
        }
        return userConnectionString;
    }

    private boolean isAuthEnabled() {
        return getConnectionString().getCredential() != null;
    }

    private  boolean isReplicaSetOrSharded() {
        try (MongoClient mongoClient = MongoClients.create(getConnectionString())) {
            Document isMaster = mongoClient.getDatabase("admin").runCommand(BsonDocument.parse("{isMaster: 1}"));
            return isMaster.containsKey("setName") || isMaster.get("msg", "").equals("isdbgrid");
        } catch (Exception e) {
            return false;
        }
    }

    private ConfigValue getConfigValue(final Config config, final String configName) {
        return config.configValues().stream().filter(cv -> cv.name().equals(configName)).collect(Collectors.toList()).get(0);
    }

    private String getDatabaseName() {
        String databaseName = getConnectionString().getDatabase();
        return databaseName != null ? databaseName : DEFAULT_DATABASE_NAME;
    }

    private ConnectionString getConnectionString() {
        String mongoURIProperty = System.getProperty(URI_SYSTEM_PROPERTY_NAME);
        String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty() ? DEFAULT_URI : mongoURIProperty;
        return new ConnectionString(mongoURIString);
    }

    private Map<String, String> createSinkProperties() {
        return createSinkProperties(getConnectionString().toString());
    }

    private Map<String, String> createSinkProperties(final String connectionString) {
        Map<String, String> properties = createProperties(connectionString);
        properties.put("topics", "test");
        properties.put(MongoSinkTopicConfig.DATABASE_CONFIG, "test");
        properties.put(MongoSinkTopicConfig.COLLECTION_CONFIG, "test");
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
