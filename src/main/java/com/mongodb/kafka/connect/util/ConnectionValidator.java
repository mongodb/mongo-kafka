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

import static com.mongodb.kafka.connect.util.ServerApiConfig.setServerApi;
import static java.lang.String.format;
import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;

public final class ConnectionValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionValidator.class);
  private static final Document CONNECTION_STATUS =
      Document.parse("{connectionStatus: 1, showPrivileges: true}");
  private static final String ROLES_INFO =
      "{rolesInfo: '%s', showPrivileges: true, showBuiltinRoles: true}";
  private static final String AUTH_INFO = "authInfo";
  private static final String AUTH_USER_PRIVILEGES = "authenticatedUserPrivileges";
  private static final String AUTH_USER_ROLES = "authenticatedUserRoles";
  private static final String INHERITED_PRIVILEGES = "inheritedPrivileges";

  public static Optional<MongoClient> validateCanConnect(
      final Config config, final String connectionStringConfigName) {
    Optional<ConfigValue> optionalConnectionString =
        ConfigHelper.getConfigByName(config, connectionStringConfigName);
    if (optionalConnectionString.isPresent()
        && optionalConnectionString.get().errorMessages().isEmpty()) {
      ConfigValue configValue = optionalConnectionString.get();

      AtomicBoolean connected = new AtomicBoolean();
      CountDownLatch latch = new CountDownLatch(1);
      ConnectionString connectionString = new ConnectionString((String) configValue.value());
      MongoClientSettings.Builder mongoClientSettingsBuilder =
          MongoClientSettings.builder().applyConnectionString(connectionString);
      setServerApi(mongoClientSettingsBuilder, config);

      MongoClientSettings mongoClientSettings =
          mongoClientSettingsBuilder
              .applyToClusterSettings(
                  b ->
                      b.addClusterListener(
                          new ClusterListener() {
                            @Override
                            public void clusterOpening(final ClusterOpeningEvent event) {}

                            @Override
                            public void clusterClosed(final ClusterClosedEvent event) {}

                            @Override // Different database than has permissions for
                            public void clusterDescriptionChanged(
                                final ClusterDescriptionChangedEvent event) {
                              ReadPreference readPreference =
                                  connectionString.getReadPreference() != null
                                      ? connectionString.getReadPreference()
                                      : ReadPreference.primaryPreferred();
                              if (!connected.get()
                                  && event.getNewDescription().hasReadableServer(readPreference)) {
                                connected.set(true);
                                latch.countDown();
                              }
                            }
                          }))
              .build();

      long latchTimeout =
          mongoClientSettings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS) + 500;
      MongoClient mongoClient = MongoClients.create(mongoClientSettings);

      try {
        if (!latch.await(latchTimeout, TimeUnit.MILLISECONDS)) {
          configValue.addErrorMessage("Unable to connect to the server.");
          mongoClient.close();
        }
      } catch (InterruptedException e) {
        mongoClient.close();
        throw new ConnectException(e);
      }

      if (configValue.errorMessages().isEmpty()) {
        return Optional.of(mongoClient);
      }
    }
    return Optional.empty();
  }

  /**
   * Validates that the user has the required action permissions
   *
   * <p>Uses the connection status privileges information to check the required action permissions
   * See: https://docs.mongodb.com/manual/reference/command/connectionStatus
   */
  public static void validateUserHasActions(
      final MongoClient mongoClient,
      final MongoCredential credential,
      final List<String> actions,
      final String databaseName,
      final String collectionName,
      final String configName,
      final Config config) {

    if (credential == null) {
      return;
    }

    try {
      Document connectionStatus =
          mongoClient.getDatabase(credential.getSource()).runCommand(CONNECTION_STATUS);

      Document authInfo = connectionStatus.get(AUTH_INFO, new Document());

      List<Document> authenticatedUserPrivileges =
          authInfo.getList(AUTH_USER_PRIVILEGES, Document.class, emptyList());
      List<String> unsupportedActions =
          removeUserActions(
              authenticatedUserPrivileges,
              credential.getSource(),
              databaseName,
              collectionName,
              actions);

      // Check against the users roles for permissions
      unsupportedActions =
          removeRoleActions(
              mongoClient, credential, databaseName, collectionName, authInfo, unsupportedActions);
      if (unsupportedActions.isEmpty()) {
        return;
      }

      String missingPermissions = String.join(", ", unsupportedActions);
      ConfigHelper.getConfigByName(config, configName)
          .ifPresent(
              c ->
                  c.addErrorMessage(
                      format(
                          "Invalid user permissions. Missing the following action permissions: %s",
                          missingPermissions)));
    } catch (MongoSecurityException e) {
      ConfigHelper.getConfigByName(config, configName)
          .ifPresent(
              c ->
                  c.addErrorMessage(
                      "Invalid user permissions authentication failed. " + e.getMessage()));
    } catch (Exception e) {
      LOGGER.warn("Permission validation failed due to: {}", e.getMessage(), e);
    }
  }

  /** Checks the users privileges list and removes any that are supported */
  private static List<String> removeUserActions(
      final List<Document> privileges,
      final String authSource,
      final String databaseName,
      final String collectionName,
      final List<String> userActions) {
    if (privileges.isEmpty() || userActions.isEmpty()) {
      return userActions;
    }

    List<String> unsupportedUserActions = new ArrayList<>(userActions);
    for (final Document privilege : privileges) {
      Document resource = privilege.get("resource", new Document());
      if (resource.containsKey("cluster") && resource.getBoolean("cluster")) {
        unsupportedUserActions.removeAll(privilege.getList("actions", String.class, emptyList()));
      } else if (resource.containsKey("db") && resource.containsKey("collection")) {
        String database = resource.getString("db");
        String collection = resource.getString("collection");

        boolean resourceMatches = false;
        boolean collectionMatches = collection.isEmpty() || collection.equals(collectionName);
        if (database.isEmpty() && collectionMatches) {
          resourceMatches = true;
        } else if (database.equals(authSource) && collection.isEmpty()) {
          resourceMatches = true;
        } else if (database.equals(databaseName) && collectionMatches) {
          resourceMatches = true;
        }

        if (resourceMatches) {
          unsupportedUserActions.removeAll(privilege.getList("actions", String.class, emptyList()));
        }
      }

      if (unsupportedUserActions.isEmpty()) {
        break;
      }
    }

    return unsupportedUserActions;
  }

  /**
   * Checks the roles info document for matching actions and removes them from the provided list
   *
   * <p>See: https://docs.mongodb.com/manual/reference/command/rolesInfo
   */
  private static List<String> removeRoleActions(
      final MongoClient mongoClient,
      final MongoCredential credential,
      final String databaseName,
      final String collectionName,
      final Document authInfo,
      final List<String> actions) {

    if (actions.isEmpty()) {
      return actions;
    }

    List<String> unsupportedActions = new ArrayList<>(actions);
    for (final Document userRole : authInfo.getList(AUTH_USER_ROLES, Document.class, emptyList())) {
      Document rolesInfo =
          mongoClient
              .getDatabase(userRole.getString("db"))
              .runCommand(Document.parse(format(ROLES_INFO, userRole.getString("role"))));
      for (final Document roleInfo : rolesInfo.getList("roles", Document.class, emptyList())) {
        unsupportedActions =
            removeUserActions(
                roleInfo.getList(INHERITED_PRIVILEGES, Document.class, emptyList()),
                credential.getSource(),
                databaseName,
                collectionName,
                unsupportedActions);
        if (unsupportedActions.isEmpty()) {
          return unsupportedActions;
        }
      }

      if (unsupportedActions.isEmpty()) {
        return unsupportedActions;
      }
    }
    return unsupportedActions;
  }

  private ConnectionValidator() {}
}
