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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.connection.SslSettings;

public final class SslConfigs {

  static final Logger LOGGER = LoggerFactory.getLogger(SslConfigs.class);

  private static final String EMPTY_STRING = "";

  public static final String CONNECTION_SSL_TRUSTSTORE_CONFIG = "connection.ssl.truststore";
  private static final String CONNECTION_SSL_TRUSTSTORE_DOC =
      "A trust store certificate location to be used for SSL enabled connections";
  public static final String CONNECTION_SSL_TRUSTSTORE_DEFAULT = EMPTY_STRING;
  private static final String CONNECTION_SSL_TRUSTSTORE_DISPLAY = "SSL TrustStore";

  public static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG =
      "connection.ssl.truststorePassword";
  private static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_DOC =
      "A trust store password to be used for SSL enabled connections";
  public static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_DEFAULT = EMPTY_STRING;
  private static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_DISPLAY =
      "SSL TrustStore Password";

  public static final String CONNECTION_SSL_KEYSTORE_CONFIG = "connection.ssl.keystore";
  private static final String CONNECTION_SSL_KEYSTORE_DOC =
      "A key store certificate location to be used for SSL enabled connections";
  public static final String CONNECTION_SSL_KEYSTORE_DEFAULT = EMPTY_STRING;
  private static final String CONNECTION_SSL_KEYSTORE_DISPLAY = "SSL KeyStore";

  public static final String CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG =
      "connection.ssl.keystorePassword";
  private static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DOC =
      "A key store password to be used for SSL enabled connections";
  public static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DEFAULT = EMPTY_STRING;
  private static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DISPLAY = "SSL KeyStore Password";

  public static ConfigDef addSslConfigDef(final ConfigDef configDef) {

    String group = "SSL";
    int orderInGroup = 0;
    configDef
        .define(
            CONNECTION_SSL_TRUSTSTORE_CONFIG,
            Type.STRING,
            CONNECTION_SSL_TRUSTSTORE_DEFAULT,
            Importance.MEDIUM,
            CONNECTION_SSL_TRUSTSTORE_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            CONNECTION_SSL_TRUSTSTORE_DISPLAY)
        .define(
            CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG,
            Type.PASSWORD,
            CONNECTION_SSL_TRUSTSTORE_PASSWORD_DEFAULT,
            Importance.MEDIUM,
            CONNECTION_SSL_TRUSTSTORE_PASSWORD_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            CONNECTION_SSL_TRUSTSTORE_PASSWORD_DISPLAY)
        .define(
            CONNECTION_SSL_KEYSTORE_CONFIG,
            Type.STRING,
            CONNECTION_SSL_KEYSTORE_DEFAULT,
            Importance.MEDIUM,
            CONNECTION_SSL_KEYSTORE_DOC,
            group,
            ++orderInGroup,
            Width.LONG,
            CONNECTION_SSL_KEYSTORE_DISPLAY)
        .define(
            CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG,
            Type.PASSWORD,
            CONNECTION_SSL_KEYSTORE_PASSWORD_DEFAULT,
            Importance.MEDIUM,
            CONNECTION_SSL_KEYSTORE_PASSWORD_DOC,
            group,
            ++orderInGroup,
            Width.MEDIUM,
            CONNECTION_SSL_KEYSTORE_PASSWORD_DISPLAY);
    return configDef;
  }

  /**
   * Set key store and trust store parameters
   *
   * @param sslSettingsBuilder - SSL Context Builder from MogoClient
   * @param config - Sink our Source Connector properties with key/trust store parameters
   */
  public static void setupSsl(
      final SslSettings.Builder sslSettingsBuilder, final AbstractConfig config) {
    try {
      TrustManager[] trustManagers = null;
      KeyManager[] keyManagers = null;

      // trust store configuration should be applied only if supplied:
      String storePath = config.getString(CONNECTION_SSL_TRUSTSTORE_CONFIG);
      Password storePassword = config.getPassword(CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG);

      storePath = storePath != null ? storePath.trim() : null;
      if (storePath != null && !storePath.isEmpty()) {
        try (InputStream trustStoreInputStream = Files.newInputStream(Paths.get(storePath))) {

          KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
          trustStore.load(trustStoreInputStream, storePassword.value().trim().toCharArray());

          TrustManagerFactory trustManagerFactory =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          trustManagerFactory.init(trustStore);
          trustManagers = trustManagerFactory.getTrustManagers();
        }
      }

      // let's do the same to the key store configuration:
      storePath = config.getString(CONNECTION_SSL_KEYSTORE_CONFIG);
      storePassword = config.getPassword(CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG);

      storePath = storePath != null ? storePath.trim() : null;
      if (storePath != null && !storePath.isEmpty()) {
        try (InputStream keyStoreInputStream = Files.newInputStream(Paths.get(storePath))) {

          char[] pwd = storePassword.value().trim().toCharArray();
          KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
          keyStore.load(keyStoreInputStream, pwd);

          KeyManagerFactory keyManagerFactory =
              KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          keyManagerFactory.init(keyStore, pwd);
          keyManagers = keyManagerFactory.getKeyManagers();
        }
      }

      if (trustManagers == null && keyManagers == null) {
        // either key or trust managers can be null, and SSLContext can be updated with nulls
        // in this case the installed security providers will
        // be searched for the highest priority implementation of the appropriate factory.
        // But, if SSL configuration parameters aren't provided (both are null),
        // it's better to leave SSLContext without a change at all.
        return;
      }

      SSLContext sslContext = getSslContext();
      sslContext.init(keyManagers, trustManagers, null);
      sslSettingsBuilder.context(sslContext);
    } catch (Exception e) {
      throw new ConnectException("Failed to initialize SSLContext.", e);
    }
  }

  /**
   * @return SSLContext configured with TLSv1.3 or TLSv1.2
   * @throws java.security.NoSuchAlgorithmException if neither TLSv1.3 nor TLSv1.2 is available
   */
  static SSLContext getSslContext() throws java.security.NoSuchAlgorithmException {
    try {
      return SSLContext.getInstance("TLSv1.3");
    } catch (java.security.NoSuchAlgorithmException e) {
      // TLSv1.3 not available, fall back to TLSv1.2
      return SSLContext.getInstance("TLSv1.2");
    }
  }

  // Utility classes should not have a public or default constructor
  private SslConfigs() {}
}
