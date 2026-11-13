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

package com.mongodb.kafka.connect.util;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectionUriBlocklistTest {

  private static final String CONNECTION_URI = "connection.uri";

  @Test
  @DisplayName("Should default to blocking the file-reading connection string options")
  void testDefaultBlockedOptions() {
    assertEquals(
        asList("tlsCertificateKeyFile", "tlsCertificateKeyFilePassword"),
        ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS);
  }

  @Test
  @DisplayName("Should allow authentication mechanisms that use credentials from the uri")
  void testNonAmbientMechanismsAreAllowed() {
    assertNull(
        ConnectionUriBlocklist.findAmbientMechanismProblem("mongodb://localhost:27017/", false));
    assertNull(
        ConnectionUriBlocklist.findAmbientMechanismProblem(
            "mongodb://localhost:27017/?authMechanism=SCRAM-SHA-256", false));
    assertNull(
        ConnectionUriBlocklist.findAmbientMechanismProblem(
            "mongodb://localhost:27017/?authMechanism=DEFAULT", false));
    assertNull(
        ConnectionUriBlocklist.findAmbientMechanismProblem(
            "mongodb://localhost:27017/?authMechanism=PLAIN", false));
  }

  @Test
  @DisplayName("Should reject MONGODB-AWS even with credentials supplied in the uri")
  void testMongoDbAwsWithCredentialsIsBlocked() {
    assertAmbient(
        "mongodb://accessKey:secretKey@localhost:27017/?authMechanism=MONGODB-AWS", "MONGODB-AWS");
    assertAmbient(
        "mongodb+srv://accessKey:secretKey@localhost:27017/?authMechanism=MONGODB-AWS",
        "MONGODB-AWS");
  }

  @Test
  @DisplayName("Should reject MONGODB-AWS without credentials supplied in the uri")
  void testMongoDbAwsWithoutCredentialsIsBlocked() {
    assertAmbient("mongodb://localhost:27017/?authMechanism=MONGODB-AWS", "MONGODB-AWS");
    assertAmbient("mongodb://localhost:27017/?authmechanism=MONGODB-AWS", "MONGODB-AWS");
    assertAmbient("mongodb://accessKey@localhost:27017/?authMechanism=MONGODB-AWS", "MONGODB-AWS");
    assertAmbient("mongodb://:secretKey@localhost:27017/?authMechanism=MONGODB-AWS", "MONGODB-AWS");
    assertAmbient("mongodb+srv://localhost:27017/?authMechanism=MONGODB-AWS", "MONGODB-AWS");
  }

  @Test
  @DisplayName("Should reject authentication mechanisms that use the host environment")
  void testAmbientMechanismsAreBlocked() {
    assertAmbient("mongodb://localhost:27017/?authMechanism=MONGODB-OIDC", "MONGODB-OIDC");
    assertAmbient(
        "mongodb://localhost:27017/?authMechanism=MONGODB-OIDC"
            + "&authMechanismProperties=ENVIRONMENT:azure",
        "MONGODB-OIDC");
    assertAmbient("mongodb://localhost:27017/?authMechanism=GSSAPI", "GSSAPI");
    assertAmbient("mongodb://localhost:27017/?authMechanism=MONGODB-X509", "MONGODB-X509");
    // Case-insensitive
    assertAmbient("mongodb://localhost:27017/?authMechanism=gssapi", "GSSAPI");
    assertAmbient("mongodb://localhost:27017/?AUTHMECHANISM=mongodb-aws", "MONGODB-AWS");
  }

  @Test
  @DisplayName("Should use the last value for duplicated options, as the driver does")
  void testDuplicatedOptionsUseLastValue() {
    assertEquals(
        "MONGODB-AWS",
        ConnectionUriBlocklist.getOption(
            "mongodb://localhost:27017/?authMechanism=SCRAM-SHA-256&authMechanism=MONGODB-AWS",
            "authMechanism"));
    // A safe first value must not mask the ambient mechanism the driver actually uses
    assertAmbient(
        "mongodb://localhost:27017/?authMechanism=SCRAM-SHA-256&authMechanism=MONGODB-AWS",
        "MONGODB-AWS");
    // A blocked first value does not matter when the driver uses a safe last value
    assertNull(
        ConnectionUriBlocklist.findAmbientMechanismProblem(
            "mongodb://localhost:27017/?authMechanism=MONGODB-AWS&authMechanism=SCRAM-SHA-256",
            false));
  }

  @Test
  @DisplayName("Should report multiple problems together")
  void testMultipleProblemsReportedTogether() {
    Config config =
        validateConnectionUri(
            "mongodb://localhost:27017/?authMechanism=MONGODB-X509"
                + "&tlsCertificateKeyFile=/etc/cert.pem&tlsCertificateKeyFilePassword=secret");
    String errorMessages = getErrorMessages(config);
    assertTrue(
        errorMessages.contains("MONGODB-X509"), "expected the mechanism, got: " + errorMessages);
    assertTrue(
        errorMessages.contains("'tlsCertificateKeyFile'"),
        "expected the first option, got: " + errorMessages);
    assertTrue(
        errorMessages.contains("'tlsCertificateKeyFilePassword'"),
        "expected the second option, got: " + errorMessages);
    assertTrue(
        errorMessages.contains(
            "options are blocked by the 'connection.uri.blocklist' configuration"),
        "expected a single grouped sentence for the options, got: " + errorMessages);
    assertFalse(
        errorMessages.contains("To allow it,"),
        "expected the plural remedy once, got: " + errorMessages);
  }

  @Test
  @DisplayName("Should read connection string option values case-insensitively")
  void testGetOption() {
    assertEquals(
        "MONGODB-AWS",
        ConnectionUriBlocklist.getOption(
            "mongodb://localhost:27017/?authmechanism=MONGODB-AWS&x=y", "authMechanism"));
    assertEquals(
        "rs0",
        ConnectionUriBlocklist.getOption(
            "mongodb://localhost:27017/?replicaSet=rs0", "REPLICASET"));
    assertNull(ConnectionUriBlocklist.getOption("mongodb://localhost:27017/?x=y", "authMechanism"));
  }

  @Test
  @DisplayName("Should find blocked options in the connection string")
  void testFindBlockedOptions() {
    assertBlocked(
        "mongodb://localhost:27017/?tlsCertificateKeyFile=/etc/passwd&connectTimeoutMS=1000",
        "tlsCertificateKeyFile");
    assertBlocked(
        "mongodb://localhost:27017/?tlsCertificateKeyFilePassword=secret",
        "tlsCertificateKeyFilePassword");
  }

  @Test
  @DisplayName("Should not match connection strings without a query")
  void testConnectionStringsWithoutQuery() {
    assertEquals(
        emptyBlocked(),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://user:secret@localhost:27017",
            ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS));
  }

  @Test
  @DisplayName("Should allow any options when the blocklist is empty")
  void testEmptyBlocklistDisablesCheck() {
    assertEquals(
        emptyBlocked(),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://localhost:27017/?tlsCertificateKeyFile=/etc/passwd", emptyList()));
  }

  @Test
  @DisplayName("Should use a custom blocklist")
  void testCustomBlocklist() {
    // Only options in the blocklist are reported
    assertEquals(
        blocked("replicaSet"),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://localhost:27017/?replicaSet=rs0&proxyHost=attacker.example",
            singletonList("replicaSet")));
    assertEquals(
        blocked("proxyHost", "tlsInsecure"),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://localhost:27017/?proxyHost=attacker.example&tlsInsecure=true",
            asList("proxyHost", "tlsInsecure")));
  }

  @Test
  @DisplayName("Should throw without exposing the connection string")
  void testValidateConnectionStringOptions() {
    String uri = "mongodb://localhost:27017/?authMechanism=MONGODB-AWS";
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () ->
                ConnectionUriBlocklist.validateConnectionStringOptions(
                    CONNECTION_URI, uri, ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS, false));
    assertTrue(
        e.getMessage().contains("MONGODB-AWS"),
        "expected the mechanism to be named, got: " + e.getMessage());
    assertFalse(e.getMessage().contains("localhost"), "error message must not contain the hosts");

    String uriWithBlockedOption =
        "mongodb://user:super-secret@localhost:27017/?tlsCertificateKeyFile=/etc/passwd";
    ConfigException e2 =
        assertThrows(
            ConfigException.class,
            () ->
                ConnectionUriBlocklist.validateConnectionStringOptions(
                    CONNECTION_URI,
                    uriWithBlockedOption,
                    ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS,
                    false));
    assertTrue(
        e2.getMessage().contains("tlsCertificateKeyFile"),
        "expected the blocked option to be named, got: " + e2.getMessage());
    assertFalse(
        e2.getMessage().contains("super-secret"), "error message must not contain the secret");
  }

  @Test
  @DisplayName("Should add error messages to the connection uri config value when rejected")
  void testValidateRawProperties() {
    Config config = validateConnectionUri("mongodb://localhost:27017/?authMechanism=MONGODB-AWS");
    assertTrue(
        getErrorMessages(config).contains("MONGODB-AWS"),
        "expected the mechanism to be named, got: " + getErrorMessages(config));

    config = validateConnectionUri("mongodb://localhost:27017/?connectTimeoutMS=1000");
    assertEquals("", getErrorMessages(config));
  }

  @Test
  @DisplayName("Should respect a raw blocklist property")
  void testValidateRawPropertiesWithCustomBlocklist() {
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTION_URI, "mongodb://localhost:27017/?proxyHost=attacker.example");
    props.put(
        ConnectionUriBlocklist.CONNECTION_URI_BLOCKLIST_CONFIG, " connectTimeoutMS , replicaSet ");
    Config config = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
    ConnectionUriBlocklist.validate(props, config);
    assertEquals("", getErrorMessages(config));

    props.put(
        ConnectionUriBlocklist.CONNECTION_URI_BLOCKLIST_CONFIG, " connectTimeoutMS , proxyHost ");
    config = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
    ConnectionUriBlocklist.validate(props, config);
    assertTrue(
        getErrorMessages(config).contains("proxyHost"),
        "expected the custom blocklist to be applied, got: " + getErrorMessages(config));
  }

  @Test
  @DisplayName("Should allow ambient mechanisms when host credentials are allowed")
  void testAmbientMechanismsAllowedWithConfig() {
    String uri = "mongodb://localhost:27017/?authMechanism=MONGODB-AWS";
    assertNull(ConnectionUriBlocklist.findAmbientMechanismProblem(uri, true));
    ConnectionUriBlocklist.validateConnectionStringOptions(
        CONNECTION_URI, uri, ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS, true);

    Map<String, String> props = new HashMap<>();
    props.put(CONNECTION_URI, uri);
    props.put(ConnectionUriBlocklist.CONNECTION_URI_ALLOW_HOST_CREDENTIALS_CONFIG, "true");
    Config config = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
    assertFalse(ConnectionUriBlocklist.validate(props, config));
    assertEquals("", getErrorMessages(config));

    // Blocked options are still enforced
    Config blockedConfig = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
    Map<String, String> propsWithBlockedOption = new HashMap<>(props);
    propsWithBlockedOption.put(
        CONNECTION_URI, "mongodb://localhost:27017/?tlsCertificateKeyFile=/etc/cert.pem");
    assertTrue(ConnectionUriBlocklist.validate(propsWithBlockedOption, blockedConfig));
    assertTrue(
        getErrorMessages(blockedConfig).contains("tlsCertificateKeyFile"),
        "expected the blocked option to be reported, got: " + getErrorMessages(blockedConfig));
  }

  @Test
  @DisplayName("Should always reject ambient mechanisms, even with an empty blocklist")
  void testAmbientMechanismsAreRejectedWithEmptyBlocklist() {
    Map<String, String> props =
        singletonMap(CONNECTION_URI, "mongodb://localhost:27017/?authMechanism=MONGODB-OIDC");
    props = new HashMap<>(props);
    props.put(ConnectionUriBlocklist.CONNECTION_URI_BLOCKLIST_CONFIG, "");
    Config config = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
    ConnectionUriBlocklist.validate(props, config);
    assertTrue(
        getErrorMessages(config).contains("MONGODB-OIDC"),
        "expected the ambient mechanism to be rejected, got: " + getErrorMessages(config));
  }

  private static Config validateConnectionUri(final String uri) {
    Config config = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
    ConnectionUriBlocklist.validate(singletonMap(CONNECTION_URI, uri), config);
    return config;
  }

  private static String getErrorMessages(final Config config) {
    Optional<ConfigValue> configValue = ConfigHelper.getConfigByName(config, CONNECTION_URI);
    assertTrue(configValue.isPresent());
    return String.join("; ", configValue.get().errorMessages());
  }

  private static void assertAmbient(final String connectionString, final String mechanism) {
    String problem = ConnectionUriBlocklist.findAmbientMechanismProblem(connectionString, false);
    assertTrue(
        problem != null && problem.contains(mechanism),
        "expected " + mechanism + " to be rejected, got: " + problem);
  }

  private static void assertBlocked(final String connectionString, final String... expected) {
    assertEquals(
        blocked(expected),
        ConnectionUriBlocklist.findBlockedOptions(
            connectionString, ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS));
  }

  private static Set<String> emptyBlocked() {
    return new LinkedHashSet<>();
  }

  private static Set<String> blocked(final String... values) {
    return new LinkedHashSet<>(asList(values));
  }
}
