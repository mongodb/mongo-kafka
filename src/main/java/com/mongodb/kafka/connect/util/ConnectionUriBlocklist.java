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

  private static final String AMBIENT_MECHANISM_PROBLEM =
      "The '%s' authentication mechanism is potentially unsafe and is not allowed.";
  private static final String BLOCKED_OPTION_PROBLEM =
      "The %s connection URI option is potentially unsafe and is not allowed. To allow it, "
          + "remove it from the '%s' configuration.";
  private static final String BLOCKED_OPTIONS_PROBLEM =
      "The %s connection URI options are potentially unsafe and are not allowed. To allow them, "
          + "remove them from the '%s' configuration.";

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

  public static boolean validate(final AbstractConfig connectorConfig, final Config config) {
    List<String> blocklist = connectorConfig.getList(CONNECTION_URI_BLOCKLIST_CONFIG);
    String connectionString = connectorConfig.getPassword(CONNECTION_URI_CONFIG).value();
    return addErrorMessagesIfInvalid(connectionString, blocklist, config);
  }

  public static boolean validate(
      final Map<String, String> connectorProperties, final Config config) {
    String blocklistValue = connectorProperties.get(CONNECTION_URI_BLOCKLIST_CONFIG);
    List<String> blocklist =
        blocklistValue == null ? DEFAULT_BLOCKED_OPTIONS : parseList(blocklistValue);
    String connectionString = connectorProperties.getOrDefault(CONNECTION_URI_CONFIG, "");
    return addErrorMessagesIfInvalid(connectionString, blocklist, config);
  }

  public static void validateConnectionStringOptions(
      final String configName, final String connectionString, final List<String> blocklist) {
    List<String> problems = findProblems(connectionString, blocklist);
    if (!problems.isEmpty()) {
      throw new ConnectConfigException(configName, Password.HIDDEN, String.join(" ", problems));
    }
  }

  private static boolean addErrorMessagesIfInvalid(
      final String connectionString, final List<String> blocklist, final Config config) {
    List<String> problems = findProblems(connectionString, blocklist);
    if (problems.isEmpty()) {
      return false;
    }
    String errorMessage = String.join(" ", problems);
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
    Set<String> blockedOptions = findBlockedOptions(connectionString, blocklist);
    if (!blockedOptions.isEmpty()) {
      problems.add(
          format(
              blockedOptions.size() == 1 ? BLOCKED_OPTION_PROBLEM : BLOCKED_OPTIONS_PROBLEM,
              joinOptions(blockedOptions),
              CONNECTION_URI_BLOCKLIST_CONFIG));
    }
    return problems;
  }

  private static String joinOptions(final Set<String> options) {
    List<String> quotedOptions =
        options.stream().map(o -> "'" + o + "'").collect(Collectors.toList());
    if (quotedOptions.size() == 1) {
      return quotedOptions.get(0);
    }
    return String.join(", ", quotedOptions.subList(0, quotedOptions.size() - 1))
        + " and "
        + quotedOptions.get(quotedOptions.size() - 1);
  }

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
        return format(AMBIENT_MECHANISM_PROBLEM, "MONGODB-AWS");
      case "MONGODB-OIDC":
        return format(AMBIENT_MECHANISM_PROBLEM, "MONGODB-OIDC");
      case "GSSAPI":
        return format(AMBIENT_MECHANISM_PROBLEM, "GSSAPI");
      case "MONGODB-X509":
        return format(AMBIENT_MECHANISM_PROBLEM, "MONGODB-X509");
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
    Set<String> seenOptions = new LinkedHashSet<>();
    for (String option : connectionString.substring(queryStart + 1).split("&")) {
      int valueStart = option.indexOf('=');
      String optionName = (valueStart >= 0 ? option.substring(0, valueStart) : option).trim();
      if (!optionName.isEmpty()
          && lowerCaseBlocklist.contains(optionName.toLowerCase(Locale.ROOT))
          && seenOptions.add(optionName.toLowerCase(Locale.ROOT))) {
        // Preserve the option name as written in the connection string for the error message
        blockedOptions.add(optionName);
      }
    }
    return blockedOptions;
  }

  /**
   * Returns the value of a connection string option, or null if the option is not present. Option
   * names are matched case-insensitively, and the last value is returned for duplicated options,
   * which is how the driver resolves them.
   */
  static String getOption(final String connectionString, final String optionName) {
    int queryStart = connectionString.indexOf('?');
    if (queryStart < 0) {
      return null;
    }
    String lowerCaseOptionName = optionName.toLowerCase(Locale.ROOT);
    String value = null;
    for (String option : connectionString.substring(queryStart + 1).split("&")) {
      int valueStart = option.indexOf('=');
      String name =
          (valueStart >= 0 ? option.substring(0, valueStart) : option).trim().toLowerCase(ROOT);
      if (name.equals(lowerCaseOptionName)) {
        value = valueStart >= 0 ? option.substring(valueStart + 1) : "";
      }
    }
    return value;
  }

  private static List<String> parseList(final String listValue) {
    return Arrays.stream(listValue.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  private ConnectionUriBlocklist() {}
}
