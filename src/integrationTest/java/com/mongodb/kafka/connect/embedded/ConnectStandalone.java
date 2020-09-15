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

import static org.apache.kafka.common.utils.Utils.sleep;

import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectStandalone {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectStandalone.class);

  private final String connectionString;
  private final Herder herder;
  private final Connect connect;

  private final ResettableOffsetStore resettableOffsetStore;

  @SuppressWarnings("unchecked")
  ConnectStandalone(final Properties workerProperties) {
    Time time = Time.SYSTEM;
    LOGGER.info("Kafka Connect standalone worker initializing ...");
    long initStart = time.hiResClockMs();
    WorkerInfo initInfo = new WorkerInfo();
    initInfo.logAll();

    Map<String, String> workerProps = (Map) workerProperties;
    resettableOffsetStore = new ResettableOffsetStore();

    LOGGER.info("Scanning for plugin classes. This might take a moment ...");
    Plugins plugins = new Plugins(workerProps);
    plugins.compareAndSwapWithDelegatingLoader();
    StandaloneConfig config = new StandaloneConfig(workerProps);

    String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);
    LOGGER.debug("Kafka cluster ID: {}", kafkaClusterId);

    RestServer rest = new RestServer(config);
    rest.initializeServer();
    URI advertisedUrl = rest.advertisedUrl();
    String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

    ConnectorClientConfigOverridePolicy clientConfigOverridePolicy =
        new NoneConnectorClientConfigOverridePolicy();
    Worker worker =
        new Worker(
            workerId, time, plugins, config, resettableOffsetStore, clientConfigOverridePolicy);
    this.herder = new StandaloneHerder(worker, kafkaClusterId, clientConfigOverridePolicy);
    connectionString = advertisedUrl.toString() + herder.kafkaClusterId();

    this.connect = new Connect(herder, rest);
    LOGGER.info(
        "Kafka Connect standalone worker initialization took {}ms",
        time.hiResClockMs() - initStart);
  }

  String getConnectionString() {
    return connectionString;
  }

  void start() {
    connect.start();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  void addConnector(final String name, final Properties properties) {
    FutureCallback<Herder.Created<ConnectorInfo>> cb =
        new FutureCallback<>(
            (error, info) -> {
              if (error != null) {
                LOGGER.error("Failed to create job for {}", properties);
              } else {
                LOGGER.info("Created connector {}", info.result().name());
              }
            });
    try {
      herder.putConnectorConfig(name, (Map) properties, true, cb);
      cb.get();
      sleep(5000);
    } catch (Exception e) {
      LOGGER.error("Failed to add connector for {}", properties);
      throw new ConnectorConfigurationException(e);
    }
  }

  void restartConnector(final String name) {
    FutureCallback<Void> cb =
        new FutureCallback<>(
            (error, info) -> {
              if (error != null) {
                LOGGER.error("Failed to restart connector: {}", name);
              } else {
                LOGGER.info("Restarted connector {}", name);
              }
            });
    try {
      herder.restartConnector(name, cb);
      cb.get();
    } catch (NotFoundException e) {
      // Ignore
    } catch (Exception e) {
      if (!(e.getCause() instanceof NotFoundException)) {
        throw new ConnectorConfigurationException(e);
      }
    }
  }

  void deleteConnector(final String name) {
    FutureCallback<Herder.Created<ConnectorInfo>> cb =
        new FutureCallback<>(
            (error, info) -> {
              if (error != null) {
                LOGGER.error("Failed to delete connector: {}", name);
              } else {
                LOGGER.info("Deleted connector {}", name);
              }
            });
    try {
      herder.deleteConnectorConfig(name, cb);
      cb.get();
    } catch (NotFoundException e) {
      // Ignore
    } catch (Exception e) {
      if (!(e.getCause() instanceof NotFoundException)) {
        throw new ConnectorConfigurationException(e);
      }
    }
  }

  void resetOffsets() {
    resettableOffsetStore.reset();
  }

  void stop() {
    LOGGER.debug("Connect Standalone stop called");
    connect.stop();
    connect.awaitStop();
  }

  class ConnectorConfigurationException extends RuntimeException {
    ConnectorConfigurationException(final Throwable cause) {
      super(cause);
    }
  }

  private static class ResettableOffsetStore extends MemoryOffsetBackingStore {

    void reset() {
      data.clear();
    }
  }
}
