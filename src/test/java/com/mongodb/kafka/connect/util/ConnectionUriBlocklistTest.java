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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
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
  @DisplayName("Should return the default blocked options")
  void testDefaultBlockedOptions() {
    assertEquals(
        Arrays.asList(
            "authMechanism",
            "authMechanismProperties",
            "proxyHost",
            "proxyPort",
            "proxyUsername",
            "proxyPassword",
            "tlsCertificateKeyFile",
            "tlsCertificateKeyFilePassword",
            "tlsInsecure",
            "tlsAllowInvalidHostnames",
            "tlsAllowInvalidCertificates",
            "sslInvalidHostNameAllowed"),
        ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS);
  }

  @Test
  @DisplayName("Should find blocked options in the connection string")
  void testFindBlockedOptions() {
    assertBlocked(
        "mongodb://user:secret@localhost:27017/?authMechanism=MONGODB-AWS", "authmechanism");
    assertBlocked(
        "mongodb://localhost:27017/?replicaSet=rs0&proxyHost=attacker.example", "proxyhost");
    assertBlocked(
        "mongodb://localhost:27017/?tlsCertificateKeyFile=/etc/passwd&connectTimeoutMS=1000",
        "tlscertificatekeyfile");
  }

  @Test
  @DisplayName("Should match option names case-insensitively")
  void testCaseInsensitiveOptionNames() {
    assertBlocked("mongodb://localhost:27017/?AUTHMECHANISM=MONGODB-AWS", "authmechanism");
    assertBlocked("mongodb://localhost:27017/?AuthMechanism=MONGODB-AWS", "authmechanism");
  }

  @Test
  @DisplayName("Should ignore option values when matching")
  void testOptionValuesAreNotMatched() {
    assertEquals(
        emptySet(),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://localhost:27017/?replicaSet=authMechanism&authSource=admin&tls=false",
            ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS));
  }

  @Test
  @DisplayName("Should not match connection strings without a query")
  void testConnectionStringsWithoutQuery() {
    assertEquals(
        emptySet(),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://user:secret@localhost:27017",
            ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS));
    assertEquals(
        emptySet(),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://localhost:27017/?", singletonList("")));
  }

  @Test
  @DisplayName("Should allow any options when the blocklist is empty")
  void testEmptyBlocklistDisablesCheck() {
    assertEquals(
        emptySet(),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://localhost:27017/?authMechanism=MONGODB-AWS", emptyList()));
  }

  @Test
  @DisplayName("Should use a custom blocklist")
  void testCustomBlocklist() {
    // Only options in the blocklist are reported
    assertEquals(
        setOf("replicaset"),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://localhost:27017/?replicaSet=rs0&authMechanism=MONGODB-AWS&proxyHost=x",
            singletonList("replicaSet")));
    assertEquals(
        setOf("authmechanism", "proxyhost"),
        ConnectionUriBlocklist.findBlockedOptions(
            "mongodb://localhost:27017/?authMechanism=MONGODB-AWS&proxyHost=attacker.example",
            asList("authMechanism", "proxyHost")));
  }

  @Test
  @DisplayName("Should throw listing the blocked options without exposing the connection string")
  void testValidateConnectionStringOptions() {
    String uri = "mongodb://user:super-secret@localhost:27017/?authMechanism=MONGODB-AWS";
    ConfigException e =
        assertThrows(
            ConfigException.class,
            () ->
                ConnectionUriBlocklist.validateConnectionStringOptions(
                    CONNECTION_URI, uri, ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS));
    assertTrue(
        e.getMessage().contains("authmechanism"),
        "expected the blocked option to be named, got: " + e.getMessage());
    assertTrue(
        e.getMessage().contains("connection.uri.blocklist"),
        "expected the blocklist config to be named, got: " + e.getMessage());
    assertFalse(e.getMessage().contains(uri), "error message must not contain the uri");
    assertFalse(
        e.getMessage().contains("super-secret"), "error message must not contain the secret");
  }

  @Test
  @DisplayName("Should block the mongodb+srv scheme while the blocklist is enabled")
  void testMongoSrvSchemeBlocked() {
    // The URI itself contains no blocked options, but DNS TXT records can supply them
    String uri = "mongodb+srv://cluster.attacker.example/";
    assertAll(
        "mongodb+srv scheme",
        () -> {
          Exception e =
              assertThrows(
                  ConfigException.class,
                  () ->
                      ConnectionUriBlocklist.validateConnectionStringOptions(
                          CONNECTION_URI, uri, ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS));
          assertTrue(
              e.getMessage().contains("mongodb+srv"),
              "expected the scheme to be named, got: " + e.getMessage());
        },
        () -> {
          Config config = validateConnectionUri(uri);
          assertTrue(
              getErrorMessages(config).contains("mongodb+srv"),
              "expected an error for the scheme, got: " + getErrorMessages(config));
        },
        () -> {
          // The scheme check is case-insensitive
          Config config = validateConnectionUri("MONGODB+SRV://cluster.attacker.example/");
          assertTrue(
              getErrorMessages(config).contains("mongodb+srv"),
              "expected an error for the scheme, got: " + getErrorMessages(config));
        },
        () -> {
          // An empty blocklist disables the scheme check too
          assertEquals(emptySet(), ConnectionUriBlocklist.findBlockedOptions(uri, emptyList()));
          Config config = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
          ConnectionUriBlocklist.validate(
              new HashMap<String, String>() {
                {
                  put(CONNECTION_URI, uri);
                  put(ConnectionUriBlocklist.CONNECTION_URI_BLOCKLIST_CONFIG, "");
                }
              },
              config);
          assertEquals("", getErrorMessages(config));
        });
  }

  @Test
  @DisplayName("Should add an error message to the connection uri config value when blocked")
  void testValidateRawProperties() {
    Config config = validateConnectionUri("mongodb://localhost:27017/?authMechanism=MONGODB-AWS");
    assertTrue(
        getErrorMessages(config).startsWith("Contains disallowed connection string options:"));

    config = validateConnectionUri("mongodb://localhost:27017/?connectTimeoutMS=1000");
    assertEquals("", getErrorMessages(config));
  }

  @Test
  @DisplayName("Should respect a raw blocklist property")
  void testValidateRawPropertiesWithCustomBlocklist() {
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTION_URI, "mongodb://localhost:27017/?authMechanism=MONGODB-AWS");
    props.put(
        ConnectionUriBlocklist.CONNECTION_URI_BLOCKLIST_CONFIG, " connectTimeoutMS , replicaSet ");
    Config config = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
    ConnectionUriBlocklist.validate(props, config);
    assertEquals("", getErrorMessages(config));

    props.put(
        ConnectionUriBlocklist.CONNECTION_URI_BLOCKLIST_CONFIG,
        " connectTimeoutMS , authMechanism ");
    config = new Config(singletonList(new ConfigValue(CONNECTION_URI)));
    ConnectionUriBlocklist.validate(props, config);
    assertTrue(
        getErrorMessages(config).contains("authmechanism"),
        "expected the custom blocklist to be applied, got: " + getErrorMessages(config));
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

  private static void assertBlocked(final String connectionString, final String... expected) {
    assertEquals(
        setOf(expected),
        ConnectionUriBlocklist.findBlockedOptions(
            connectionString, ConnectionUriBlocklist.DEFAULT_BLOCKED_OPTIONS));
  }

  private static Set<String> emptySet() {
    return new LinkedHashSet<>();
  }

  private static Set<String> setOf(final String... values) {
    return new LinkedHashSet<>(asList(values));
  }
}
