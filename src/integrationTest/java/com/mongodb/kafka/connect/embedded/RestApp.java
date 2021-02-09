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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2018 Confluent Inc.
 */
package com.mongodb.kafka.connect.embedded;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;

import org.eclipse.jetty.server.Server;

public class RestApp {

  public final Properties prop;
  public RestService restClient;
  public SchemaRegistryRestApplication restApp;
  public Server restServer;
  public String restConnect;

  public RestApp(String zkConnect, String kafkaTopic) {
    this(zkConnect, kafkaTopic, CompatibilityLevel.NONE.name, null);
  }

  public RestApp(
      String zkConnect,
      String kafkaTopic,
      String compatibilityType,
      Properties schemaRegistryProps) {
    this(zkConnect, null, kafkaTopic, compatibilityType, true, schemaRegistryProps);
  }

  public RestApp(
      String zkConnect,
      String kafkaTopic,
      String compatibilityType,
      boolean masterEligibility,
      Properties schemaRegistryProps) {
    this(zkConnect, null, kafkaTopic, compatibilityType, masterEligibility, schemaRegistryProps);
  }

  public RestApp(
      String zkConnect,
      String bootstrapBrokers,
      String kafkaTopic,
      String compatibilityType,
      boolean masterEligibility,
      Properties schemaRegistryProps) {
    prop = new Properties();
    if (schemaRegistryProps != null) {
      prop.putAll(schemaRegistryProps);
    }
    if (zkConnect != null) {
      prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    }
    if (bootstrapBrokers != null) {
      prop.setProperty(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    }
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic);
    prop.put(SchemaRegistryConfig.SCHEMA_COMPATIBILITY_CONFIG, compatibilityType);
    prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility);
  }

  public void start() throws Exception {
    restApp = new SchemaRegistryRestApplication(prop);
    restServer = restApp.createServer();
    restServer.start();
    restConnect = restServer.getURI().toString();
    if (restConnect.endsWith("/")) restConnect = restConnect.substring(0, restConnect.length() - 1);
    restClient = new RestService(restConnect);
  }

  public void stop() throws Exception {
    restClient = null;
    if (restServer != null) {
      restServer.stop();
      restServer.join();
    }
  }

  /**
   * This method must be called before calling {@code RestApp.start()} for the additional properties
   * to take affect.
   *
   * @param props the additional properties to set
   */
  public void addConfigs(Properties props) {
    prop.putAll(props);
  }

  public boolean isMaster() {
    return restApp.schemaRegistry().isMaster();
  }

  public void setMaster(SchemaRegistryIdentity schemaRegistryIdentity)
      throws SchemaRegistryException {
    restApp.schemaRegistry().setMaster(schemaRegistryIdentity);
  }

  public SchemaRegistryIdentity myIdentity() {
    return restApp.schemaRegistry().myIdentity();
  }

  public SchemaRegistryIdentity masterIdentity() {
    return restApp.schemaRegistry().masterIdentity();
  }

  public SchemaRegistry schemaRegistry() {
    return restApp.schemaRegistry();
  }
}
