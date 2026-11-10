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

import static com.mongodb.kafka.connect.util.ConfigHelper.getConfigByName;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.types.Password;

/**
 * Blocks connection string options that can make the driver use credentials from the worker's host
 * environment or otherwise leak sensitive information, for example by performing an ambient
 * credential (eg. MONGODB-AWS / MONGODB-OIDC) handshake against an arbitrary host.
 *
 * <p>The blocklist can be changed with the {@code connection.uri.blocklist} configuration; setting
 * it to an empty list disables the check.
 */
public final class ConnectionUriBlocklist {

  public static final String CONNECTION_URI_BLOCKLIST_CONFIG = "connection.uri.blocklist";
  private static final String CONNECTION_URI_CONFIG = "connection.uri";
  private static final String CONNECTION_URI_BLOCKLIST_DISPLAY = "Connection URI blocklist";

  /**
   * Connection string options disallowed by default:
   *
   * <ul>
   *   <li>{@code authMechanism} / {@code authMechanismProperties}: mechanisms such as MONGODB-AWS
   *       and MONGODB-OIDC fall back to the host's ambient cloud credentials when none are supplied
   *       in the connection string.
   *   <li>{@code proxyHost} / {@code proxyPort} / {@code proxyUsername} / {@code proxyPassword}:
   *       route all traffic, including authentication exchanges, through an arbitrary proxy.
   *   <li>{@code tlsCertificateKeyFile} / {@code tlsCertificateKeyFilePassword}: make the driver
   *       present a key file from the worker's file system to the connected server.
   *   <li>{@code tlsInsecure}, {@code tlsAllowInvalidHostnames}, {@code
   *       tlsAllowInvalidCertificates}, {@code sslInvalidHostNameAllowed}: disable server
   *       certificate checks, allowing an untrusted server to impersonate the intended one.
   * </ul>
   */
  public static final List<String> DEFAULT_BLOCKED_OPTIONS =
      unmodifiableList(
          asList(
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
              "sslInvalidHostNameAllowed"));

  private static final String MONGODB_SRV_SCHEME = "mongodb+srv://";

  private static final String CONNECTION_URI_BLOCKLIST_DOC =
      "A list of connection string options that are not allowed in 'connection.uri'. "
          + "This prevents connection strings from making the driver use credentials from the "
          + "host environment, eg. when connecting to an untrusted server. Defaults to: "
          + String.join(", ", DEFAULT_BLOCKED_OPTIONS)
          + ". While the blocklist is enabled, the '"
          + MONGODB_SRV_SCHEME
          + "' scheme is also not allowed, as it can supply options via DNS TXT records. "
          + "Set to an empty list to disable the check.";

  /**
   * Adds the {@value #CONNECTION_URI_BLOCKLIST_CONFIG} configuration.
   *
   * @param configDef the config def to add the configuration to
   * @param group the group the configuration belongs to
   * @param orderInGroup the order of the configuration within the group
   */
  public static void addConfigDef(
      final ConfigDef configDef, final String group, final int orderInGroup) {
    configDef.define(
        CONNECTION_URI_BLOCKLIST_CONFIG,
        Type.LIST,
        DEFAULT_BLOCKED_OPTIONS,
        Importance.MEDIUM,
        CONNECTION_URI_BLOCKLIST_DOC,
        group,
        orderInGroup,
        Width.MEDIUM,
        CONNECTION_URI_BLOCKLIST_DISPLAY);
  }

  /**
   * Validates the connection uri against the blocklist and adds an error message to the connection
   * uri config value if it contains blocked options.
   *
   * @param connectorConfig the connector configuration containing the connection uri and blocklist
   * @param config the validated config to add the error message to
   * @return true if the connection uri contained blocked options
   */
  public static boolean validate(final AbstractConfig connectorConfig, final Config config) {
    List<String> blocklist = connectorConfig.getList(CONNECTION_URI_BLOCKLIST_CONFIG);
    String connectionString = connectorConfig.getPassword(CONNECTION_URI_CONFIG).value();
    return addErrorMessageIfBlocked(connectionString, blocklist, config);
  }

  /**
   * Validates the raw connection uri configuration value against the blocklist and adds an error
   * message to the connection uri config value if it contains blocked options.
   *
   * @param connectorProperties the raw connector properties
   * @param config the validated config to add the error message to
   * @return true if the connection uri contained blocked options
   */
  public static boolean validate(
      final Map<String, String> connectorProperties, final Config config) {
    String blocklistValue = connectorProperties.get(CONNECTION_URI_BLOCKLIST_CONFIG);
    List<String> blocklist =
        blocklistValue == null ? DEFAULT_BLOCKED_OPTIONS : parseList(blocklistValue);
    String connectionString = connectorProperties.getOrDefault(CONNECTION_URI_CONFIG, "");
    return addErrorMessageIfBlocked(connectionString, blocklist, config);
  }

  /**
   * Throws a ConfigException if the connection string contains blocked options.
   *
   * @param configName the name of the configuration containing the connection string
   * @param connectionString the connection string to check
   * @param blocklist the connection string options that are not allowed
   */
  public static void validateConnectionStringOptions(
      final String configName, final String connectionString, final List<String> blocklist) {
    Set<String> blockedOptions = findBlockedOptions(connectionString, blocklist);
    if (!blockedOptions.isEmpty()
        || (!blocklist.isEmpty() && usesMongoSrvScheme(connectionString))) {
      throw new ConnectConfigException(configName, Password.HIDDEN, blockedMessage(blockedOptions));
    }
  }

  private static boolean addErrorMessageIfBlocked(
      final String connectionString, final List<String> blocklist, final Config config) {
    Set<String> blockedOptions = findBlockedOptions(connectionString, blocklist);
    if (blockedOptions.isEmpty()
        && (blocklist.isEmpty() || !usesMongoSrvScheme(connectionString))) {
      return false;
    }
    String errorMessage = blockedMessage(blockedOptions);
    getConfigByName(config, CONNECTION_URI_CONFIG)
        .filter(
            connectionUriConfigValue ->
                !connectionUriConfigValue.errorMessages().contains(errorMessage))
        .ifPresent(
            connectionUriConfigValue -> connectionUriConfigValue.addErrorMessage(errorMessage));
    return true;
  }

  /**
   * The mongodb+srv scheme sources options from DNS TXT records for the host, which cannot be
   * checked against the blocklist.
   */
  private static boolean usesMongoSrvScheme(final String connectionString) {
    return connectionString != null
        && connectionString.toLowerCase(Locale.ROOT).startsWith(MONGODB_SRV_SCHEME);
  }

  private static String blockedMessage(final Set<String> blockedOptions) {
    if (blockedOptions.isEmpty()) {
      return format(
          "The '%s' scheme is disallowed because connection string options can also be supplied "
              + "via DNS TXT records, which cannot be checked against '%s'.",
          MONGODB_SRV_SCHEME, CONNECTION_URI_BLOCKLIST_CONFIG);
    }
    return format(
        "Contains disallowed connection string options: %s. "
            + "Disallowed options can be configured with '%s'.",
        blockedOptions, CONNECTION_URI_BLOCKLIST_CONFIG);
  }

  static Set<String> findBlockedOptions(
      final String connectionString, final List<String> blocklist) {
    if (blocklist.isEmpty() || connectionString == null) {
      return new LinkedHashSet<>();
    }
    int queryStart = connectionString.indexOf('?');
    if (queryStart < 0) {
      return new LinkedHashSet<>();
    }
    Set<String> lowerCaseBlocklist =
        blocklist.stream().map(o -> o.trim().toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    Set<String> blockedOptions = new LinkedHashSet<>();
    for (String option : connectionString.substring(queryStart + 1).split("&")) {
      int valueStart = option.indexOf('=');
      String optionName =
          (valueStart >= 0 ? option.substring(0, valueStart) : option)
              .trim()
              .toLowerCase(Locale.ROOT);
      if (!optionName.isEmpty() && lowerCaseBlocklist.contains(optionName)) {
        blockedOptions.add(optionName);
      }
    }
    return blockedOptions;
  }

  private static List<String> parseList(final String listValue) {
    return Arrays.stream(listValue.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  private ConnectionUriBlocklist() {}
}
