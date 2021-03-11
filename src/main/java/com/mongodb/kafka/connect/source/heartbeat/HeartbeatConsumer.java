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

package com.mongodb.kafka.connect.source.heartbeat;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HeartbeatConsumer implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatConsumer.class);
  private static final String CONSUMER_GROUP_ID = "MONGODB_SOURCE_HEARTBEAT";
  private static final String CONSUMER_DESERIALIZER =
      "org.apache.kafka.common.serialization.BytesDeserializer";

  private final AtomicBoolean running;
  private final KafkaConsumer<Bytes, Bytes> consumer;
  private final String heartbeatTopicName;
  private final long heartbeatIntervalMS;

  HeartbeatConsumer(
      final List<String> bootStrapServers,
      final long heartbeatIntervalMS,
      final String heartbeatTopicName) {
    this.consumer = tryCreateConsumer(String.join(",", bootStrapServers)).orElse(null);
    this.heartbeatIntervalMS = heartbeatIntervalMS;
    this.heartbeatTopicName = heartbeatTopicName;
    this.running = new AtomicBoolean(false);
    if (consumer != null) {
      new Thread(this).start();
      LOGGER.info("Start heartbeat offset consumer");
    }
  }

  public void run() {
    running.set(true);
    consumer.subscribe(Collections.singleton(heartbeatTopicName));
    Duration pollDuration = Duration.ofMillis(heartbeatIntervalMS);
    try {
      while (running.get()) {
        if (!consumer.poll(pollDuration).isEmpty()) {
          try {
            LOGGER.info("Syncing heartbeat offsets");
            consumer.commitSync();
          } catch (CommitFailedException e) {
            // ignore any superseded commits by the connector
          }
        }
      }
    } catch (WakeupException e) {
      // Ignore exception if closing
      if (!running.get()) {
        throw e;
      }
    } catch (Exception e) {
      LOGGER.error("Heartbeat consumer exception", e);
    } finally {
      consumer.close();
    }
  }

  public void shutdown() {
    running.set(false);
    if (consumer != null) {
      consumer.wakeup();
    }
  }

  private static Optional<KafkaConsumer<Bytes, Bytes>> tryCreateConsumer(
      final String bootStrapServers) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", bootStrapServers);
    props.setProperty("group.id", CONSUMER_GROUP_ID);
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("key.deserializer", CONSUMER_DESERIALIZER);
    props.setProperty("value.deserializer", CONSUMER_DESERIALIZER);

    try {
      return Optional.of(new KafkaConsumer<>(props));
    } catch (Exception e) {
      LOGGER.error("Unable to create Heartbeat consumer", e);
      return Optional.empty();
    }
  }
}
