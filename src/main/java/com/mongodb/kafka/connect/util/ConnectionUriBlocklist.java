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
import static java.util.Locale.ROOT;

import java.util.ArrayList;
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
 * Prevents connection strings from making the driver use resources from the connector host's
 * environment on behalf of the connection string author, for example performing an ambient
 * credential (eg. MONGODB-AWS / MONGODB-OIDC) handshake against an arbitrary host.
 *
 * <p>Two checks are enforced wherever a {@code connection.uri} is used:
 *
 * <ul>
 *   <li><em>Ambient authentication mechanisms</em> are always rejected. Authentication mechanisms
 *       that fall back to the host environment when no credentials are supplied in the connection
 *       string itself let the author of the connection string make the driver reach into the host's
 *       environment. Use the {@code provider} configuration or supply credentials in the connection
 *       string instead.
 *   <li>A configurable blocklist of connection string options, defaulting to the options that make
 *       the driver read files from the host's file system. This can be changed with the {@code
 *       connection.uri.blocklist} configuration; setting it to an empty list disables the
 *       blocklist.
 * </ul>
 */
public final class ConnectionUriBlocklist {

  public static final String CONNECTION_URI_BLOCKLIST_CONFIG = "connection.uri.blocklist";
  private static final String CONNECTION_URI_CONFIG = "connection.uri";
  private static final String CONNECTION_URI_BLOCKLIST_DISPLAY = "Connection URI blocklist";

  /** Connection string options that make the driver read files from the host's file system. */
  public static final List<String> DEFAULT_BLOCKED_OPTIONS =
      unmodifiableList(asList("tlsCertificateKeyFile", "tlsCertificateKeyFilePassword"));

  private static final String CONNECTION_URI_BLOCKLIST_DOC =
      "A list of connection string options that are not allowed in 'connection.uri'. "
          + "This prevents connection strings from making the driver read files from the host "
          + "environment. Defaults to: "
          + String.join(", ", DEFAULT_BLOCKED_OPTIONS)
          + ". Set to an empty list to disable the blocklist. "
          + "Authentication mechanisms that make the driver use credentials from the host "
          + "environment are always disallowed and can not be configured with this setting.";

  private static final String MONGODB_SCHEME = "mongodb://";
  private static final String MONGODB_SRV_SCHEME = "mongodb+srv://";
  private static final String AMBIENT_MECHANISMS_MESSAGE =
      "uses the %s authentication mechanism, which makes the driver use credentials from the host "
          + "environment. Credentials must be supplied in the connection string, or use the "
          + "'provider' configuration to configure credentials on the host's behalf";

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
   * Validates the connection uri and adds error messages to the connection uri config value if it
   * uses an ambient authentication mechanism or contains blocked options.
   *
   * @param connectorConfig the connector configuration containing the connection uri and blocklist
   * @param config the validated config to add the error messages to
   * @return true if the connection uri was rejected
   */
  public static boolean validate(final AbstractConfig connectorConfig, final Config config) {
    List<String> blocklist = connectorConfig.getList(CONNECTION_URI_BLOCKLIST_CONFIG);
    String connectionString = connectorConfig.getPassword(CONNECTION_URI_CONFIG).value();
    return addErrorMessagesIfInvalid(connectionString, blocklist, config);
  }

  /**
   * Validates the raw connection uri configuration value and adds error messages to the connection
   * uri config value if it uses an ambient authentication mechanism or contains blocked options.
   *
   * @param connectorProperties the raw connector properties
   * @param config the validated config to add the error messages to
   * @return true if the connection uri was rejected
   */
  public static boolean validate(
      final Map<String, String> connectorProperties, final Config config) {
    String blocklistValue = connectorProperties.get(CONNECTION_URI_BLOCKLIST_CONFIG);
    List<String> blocklist =
        blocklistValue == null ? DEFAULT_BLOCKED_OPTIONS : parseList(blocklistValue);
    String connectionString = connectorProperties.getOrDefault(CONNECTION_URI_CONFIG, "");
    return addErrorMessagesIfInvalid(connectionString, blocklist, config);
  }

  /**
   * Throws a ConfigException if the connection string uses an ambient authentication mechanism or
   * contains blocked options.
   *
   * @param configName the name of the configuration containing the connection string
   * @param connectionString the connection string to check
   * @param blocklist the connection string options that are not allowed
   */
  public static void validateConnectionStringOptions(
      final String configName, final String connectionString, final List<String> blocklist) {
    List<String> problems = findProblems(connectionString, blocklist);
    if (!problems.isEmpty()) {
      throw new ConnectConfigException(
          configName, Password.HIDDEN, format("Is not allowed: %s.", String.join("; ", problems)));
    }
  }

  private static boolean addErrorMessagesIfInvalid(
      final String connectionString, final List<String> blocklist, final Config config) {
    List<String> problems = findProblems(connectionString, blocklist);
    if (problems.isEmpty()) {
      return false;
    }
    String errorMessage = format("Is not allowed: %s.", String.join("; ", problems));
    getConfigByName(config, CONNECTION_URI_CONFIG)
        .filter(
            connectionUriConfigValue ->
                !connectionUriConfigValue.errorMessages().contains(errorMessage))
        .ifPresent(
            connectionUriConfigValue -> connectionUriConfigValue.addErrorMessage(errorMessage));
    return true;
  }

  private static List<String> findProblems(
      final String connectionString, final List<String> blocklist) {
    List<String> problems = new ArrayList<>();
    String ambientMechanismProblem = findAmbientMechanismProblem(connectionString);
    if (ambientMechanismProblem != null) {
      problems.add(ambientMechanismProblem);
    }
    for (String option : findBlockedOptions(connectionString, blocklist)) {
      problems.add(
          format(
              "the '%s' connection string option is disallowed. Disallowed options can be "
                  + "configured with '%s'",
              option, CONNECTION_URI_BLOCKLIST_CONFIG));
    }
    return problems;
  }

  /**
   * Returns a message if the connection string uses an authentication mechanism that makes the
   * driver use credentials from the host environment, null otherwise.
   */
  static String findAmbientMechanismProblem(final String connectionString) {
    if (connectionString == null) {
      return null;
    }
    String authMechanism = getOption(connectionString, "authMechanism");
    if (authMechanism == null) {
      return null;
    }
    switch (authMechanism.toUpperCase(ROOT)) {
      case "MONGODB-AWS":
        // The driver only falls back to the host environment if the connection string supplies no
        // credentials. Explicit credentials in the connection string are always used.
        return hasCredentials(connectionString)
            ? null
            : format(AMBIENT_MECHANISMS_MESSAGE, "MONGODB-AWS");
      case "MONGODB-OIDC":
        return format(AMBIENT_MECHANISMS_MESSAGE, "MONGODB-OIDC");
      case "GSSAPI":
        return format(AMBIENT_MECHANISMS_MESSAGE, "GSSAPI");
      case "MONGODB-X509":
        return format(AMBIENT_MECHANISMS_MESSAGE, "MONGODB-X509");
      default:
        return null;
    }
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

  /**
   * Returns the value of a connection string option, or null if the option is not present. Option
   * names are matched case-insensitively.
   */
  static String getOption(final String connectionString, final String optionName) {
    int queryStart = connectionString.indexOf('?');
    if (queryStart < 0) {
      return null;
    }
    String lowerCaseOptionName = optionName.toLowerCase(Locale.ROOT);
    for (String option : connectionString.substring(queryStart + 1).split("&")) {
      int valueStart = option.indexOf('=');
      String name =
          (valueStart >= 0 ? option.substring(0, valueStart) : option).trim().toLowerCase(ROOT);
      if (name.equals(lowerCaseOptionName)) {
        return valueStart >= 0 ? option.substring(valueStart + 1) : "";
      }
    }
    return null;
  }

  /**
   * Returns whether the connection string contains credentials in the user info part of the uri,
   * eg. {@code mongodb://user:password@host}.
   */
  static boolean hasCredentials(final String connectionString) {
    String scheme =
        connectionString.startsWith(MONGODB_SRV_SCHEME)
            ? MONGODB_SRV_SCHEME
            : connectionString.startsWith(MONGODB_SCHEME) ? MONGODB_SCHEME : null;
    if (scheme == null) {
      return false;
    }
    String authority = connectionString.substring(scheme.length());
    int hostStart = Math.min(authority.indexOf('/'), authority.length());
    authority = authority.substring(0, hostStart);
    int atIndex = authority.lastIndexOf('@');
    if (atIndex < 0) {
      return false;
    }
    String userInfo = authority.substring(0, atIndex);
    int colonIndex = userInfo.indexOf(':');
    String username = colonIndex >= 0 ? userInfo.substring(0, colonIndex) : userInfo;
    String password = colonIndex >= 0 ? userInfo.substring(colonIndex + 1) : "";
    return !username.isEmpty() && !password.isEmpty();
  }

  private static List<String> parseList(final String listValue) {
    return Arrays.stream(listValue.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  private ConnectionUriBlocklist() {}
}
