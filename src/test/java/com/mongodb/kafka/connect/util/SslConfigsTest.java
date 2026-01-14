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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.connection.SslSettings;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig;

class SslConfigsTest {

  private static final String TRUSTSTORE_LOCATION = "truststore.location";
  private static final String TRUSTSTORE_PASSWORD = "truststore.password";
  private static final String KEYSTORE_LOCATION = "keystore.location";
  private static final String KEYSTORE_PASSWORD = "keystore.password";

  @Test
  void testSslMongoSourceConfig() {

    Map<String, String> originals = new HashMap<String, String>();
    originals.put(SslConfigs.CONNECTION_SSL_TRUSTSTORE_CONFIG, TRUSTSTORE_LOCATION);
    originals.put(SslConfigs.CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD);
    originals.put(SslConfigs.CONNECTION_SSL_KEYSTORE_CONFIG, KEYSTORE_LOCATION);
    originals.put(SslConfigs.CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD);

    AbstractConfig config = new MongoSourceConfig(originals);

    testSslConfigs(config);
  }

  @Test
  void testSslMongoSinkConfig() {

    Map<String, String> originals = new HashMap<String, String>();
    originals.put(SslConfigs.CONNECTION_SSL_TRUSTSTORE_CONFIG, TRUSTSTORE_LOCATION);
    originals.put(SslConfigs.CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD);
    originals.put(SslConfigs.CONNECTION_SSL_KEYSTORE_CONFIG, KEYSTORE_LOCATION);
    originals.put(SslConfigs.CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PASSWORD);
    originals.put(MongoSinkTopicConfig.DATABASE_CONFIG, "database");
    originals.put(MongoSinkConfig.TOPICS_CONFIG, "topics");

    AbstractConfig config = new MongoSinkConfig(originals);

    testSslConfigs(config);
  }

  void testSslConfigs(final AbstractConfig config) {

    assertEquals(
        TRUSTSTORE_LOCATION, config.getString(SslConfigs.CONNECTION_SSL_TRUSTSTORE_CONFIG));
    assertEquals(
        TRUSTSTORE_PASSWORD,
        config.getPassword(SslConfigs.CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG).value());
    assertEquals(KEYSTORE_LOCATION, config.getString(SslConfigs.CONNECTION_SSL_KEYSTORE_CONFIG));
    assertEquals(
        KEYSTORE_PASSWORD,
        config.getPassword(SslConfigs.CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG).value());
  }

  @Test
  @DisplayName("Ensure getSslContext returns TLSv1.3 or TLSv1.2")
  void testGetSslContextReturnsSecureProtocol() throws NoSuchAlgorithmException {
    SSLContext sslContext = SslConfigs.getSslContext();

    assertNotNull(sslContext, "SSLContext should not be null");

    String protocol = sslContext.getProtocol();
    List<String> secureProtocols = Arrays.asList("TLSv1.3", "TLSv1.2");
    assertTrue(
        secureProtocols.contains(protocol),
        "SSLContext protocol should be TLSv1.3 or TLSv1.2, but was: " + protocol);
  }

  @Test
  @DisplayName("Ensure setupSsl throws ConnectException when truststore file does not exist")
  void testSetupSslThrowsConnectExceptionForInvalidTruststore() {
    Map<String, String> originals = new HashMap<>();
    originals.put(SslConfigs.CONNECTION_SSL_TRUSTSTORE_CONFIG, "/nonexistent/truststore.jks");
    originals.put(SslConfigs.CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
    originals.put(MongoSinkTopicConfig.DATABASE_CONFIG, "database");
    originals.put(MongoSinkConfig.TOPICS_CONFIG, "topics");

    AbstractConfig config = new MongoSinkConfig(originals);
    SslSettings.Builder sslSettingsBuilder = SslSettings.builder();

    ConnectException exception =
        assertThrows(
            ConnectException.class,
            () -> SslConfigs.setupSsl(sslSettingsBuilder, config),
            "setupSsl should throw ConnectException when truststore file does not exist");

    assertTrue(
        exception.getMessage().contains("Failed to initialize SSLContext"),
        "Exception message should indicate SSLContext initialization failure");
  }

  @Test
  @DisplayName("Ensure setupSsl throws ConnectException when keystore file does not exist")
  void testSetupSslThrowsConnectExceptionForInvalidKeystore() {
    Map<String, String> originals = new HashMap<>();
    originals.put(SslConfigs.CONNECTION_SSL_KEYSTORE_CONFIG, "/nonexistent/keystore.jks");
    originals.put(SslConfigs.CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG, "password");
    originals.put(MongoSinkTopicConfig.DATABASE_CONFIG, "database");
    originals.put(MongoSinkConfig.TOPICS_CONFIG, "topics");

    AbstractConfig config = new MongoSinkConfig(originals);
    SslSettings.Builder sslSettingsBuilder = SslSettings.builder();

    ConnectException exception =
        assertThrows(
            ConnectException.class,
            () -> SslConfigs.setupSsl(sslSettingsBuilder, config),
            "setupSsl should throw ConnectException when keystore file does not exist");

    assertTrue(
        exception.getMessage().contains("Failed to initialize SSLContext"),
        "Exception message should indicate SSLContext initialization failure");
  }
}
