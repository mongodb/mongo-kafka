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
package com.mongodb.kafka.connect;

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_REGEX_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPIC_OVERRIDE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.COLLECTION_CONFIG;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import javax.management.MBeanServer;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import com.mongodb.client.model.Sorts;

import com.mongodb.kafka.connect.avro.TweetMsg;
import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.util.jmx.internal.MBeanServerUtils;

class MongoSinkConnectorIntegrationTest extends MongoKafkaTestCase {
  private static final Random RANDOM = new Random();

  @Test
  @DisplayName("Ensure sink connect saves data to MongoDB")
  void testSinkSavesAvroDataToMongoDB() {
    String topicName = getTopicName();
    KAFKA.createTopic(topicName);
    addSinkConnector(topicName);

    assertProducesMessages(topicName, getCollectionName());
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure sink saves data using multiple tasks and a single partition")
  void testSinkSavesUsingMultipleTasksWithASinglePartition() {
    String topicName = getTopicName();
    KAFKA.createTopic(topicName, 3, 1);

    Properties sinkProperties = createSinkProperties();
    sinkProperties.put(TOPICS_CONFIG, topicName);
    sinkProperties.put("tasks.max", "5");
    addSinkConnector(sinkProperties);

    assertProducesMessages(topicName, getCollectionName());
    assertCollectionOrder(true);
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure sink saves data using a single task and multiple partitions")
  void testSinkSavesUsingASingleTasksWithMultiplePartitions() {
    String topicName = getTopicName();
    int partitionCount = 3;
    KAFKA.createTopic(topicName, partitionCount, 1);

    addSinkConnector(topicName);
    assertProducesMessages(topicName, getCollectionName(), partitionCount);
    assertCollectionOrder(false);
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure sink saves data using multiple tasks and multiple partitions")
  void testSinkSavesUsingMultipleTasksWithMultiplePartitions() {
    String topicName = getTopicName();
    int partitionCount = 3;
    KAFKA.createTopic(topicName, partitionCount, 1);

    Properties sinkProperties = createSinkProperties();
    sinkProperties.put(TOPICS_CONFIG, topicName);
    sinkProperties.put("tasks.max", "5");
    addSinkConnector(sinkProperties);

    assertProducesMessages(topicName, getCollectionName(), partitionCount);
    assertCollectionOrder(false);
    assertMetrics();
  }

  @Test
  @DisplayName(
      "Ensure sink saves data to multiple collections using multiple tasks and multiple partitions")
  void testSinkSavesToMultipleCollectionsUsingMultipleTasksWithMultiplePartitions() {
    String topicName1 = getTopicName();
    String topicName2 = getTopicName();
    String collectionName1 = topicName1 + "Collection";
    String collectionName2 = topicName2 + "Collection";

    int partitionCount = 3;
    KAFKA.createTopic(topicName1);
    KAFKA.createTopic(topicName2, partitionCount, 1);

    Properties sinkProperties = createSinkProperties();
    sinkProperties.put(TOPICS_CONFIG, format("%s,%s", topicName1, topicName2));
    sinkProperties.put(
        format(TOPIC_OVERRIDE_CONFIG, topicName1, COLLECTION_CONFIG), collectionName1);
    sinkProperties.put(
        format(TOPIC_OVERRIDE_CONFIG, topicName2, COLLECTION_CONFIG), collectionName2);
    sinkProperties.put("tasks.max", "5");
    addSinkConnector(sinkProperties);

    assertProducesMessages(topicName1, collectionName1);
    assertProducesMessages(topicName2, collectionName2, partitionCount);
    assertCollectionOrder(collectionName1, true);
    assertCollectionOrder(collectionName2, false);
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure sink connect saves data to MongoDB when using regex")
  void testSinkSavesAvroDataToMongoDBWhenUsingRegex() {
    String topicName1 = "topic-regex-101";
    String topicName2 = "topic-regex-202";

    String collectionName1 = "regexColl1";
    String collectionName2 = "regexColl2";

    KAFKA.createTopic(topicName1);
    KAFKA.createTopic(topicName2);

    Properties sinkProperties = createSinkProperties();
    sinkProperties.put(TOPICS_REGEX_CONFIG, "topic\\-regex\\-(.*)");
    sinkProperties.put(
        format(TOPIC_OVERRIDE_CONFIG, topicName1, COLLECTION_CONFIG), collectionName1);
    sinkProperties.put(
        format(TOPIC_OVERRIDE_CONFIG, topicName2, COLLECTION_CONFIG), collectionName2);
    addSinkConnector(sinkProperties);

    assertProducesMessages(topicName1, collectionName1);
    assertProducesMessages(topicName2, collectionName2);
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure sink can survive a restart")
  void testSinkSurvivesARestart() {
    String topicName = getTopicName();
    KAFKA.createTopic(topicName);
    addSinkConnector(topicName);
    assertProducesMessages(topicName, getCollectionName(), true);
    assertMetrics();
  }

  private void assertMetrics() {
    Set<String> names =
        new HashSet<>(
            Arrays.asList(
                "records-received",
                "records-succeeded",
                "records-failed",
                "latest-offset-ms",
                "task-invocations",
                "task-invocations-over-1ms",
                "task-invocations-over-10ms",
                "task-invocations-over-100ms",
                "task-invocations-over-1000ms",
                "task-invocations-over-10000ms",
                "task-invocations-total-ms",
                "between-task-invocations",
                "between-task-invocations-over-1ms",
                "between-task-invocations-over-10ms",
                "between-task-invocations-over-100ms",
                "between-task-invocations-over-1000ms",
                "between-task-invocations-over-10000ms",
                "between-task-invocations-total-ms",
                "records-processing",
                "records-processing-over-1ms",
                "records-processing-over-10ms",
                "records-processing-over-100ms",
                "records-processing-over-1000ms",
                "records-processing-over-10000ms",
                "records-processing-total-ms",
                "successful-batch-writes",
                "successful-batch-writes-over-1ms",
                "successful-batch-writes-over-10ms",
                "successful-batch-writes-over-100ms",
                "successful-batch-writes-over-1000ms",
                "successful-batch-writes-over-10000ms",
                "successful-batch-writes-total-ms",
                "failed-batch-writes",
                "failed-batch-writes-over-1ms",
                "failed-batch-writes-over-10ms",
                "failed-batch-writes-over-100ms",
                "failed-batch-writes-over-1000ms",
                "failed-batch-writes-over-10000ms",
                "failed-batch-writes-total-ms"));

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    String mBeanName = "com.mongodb:type=MongoDBKafkaConnector,name=SinkTask0";
    Map<String, Map<String, Long>> mBeansMap =
        MBeanServerUtils.getMBeanAttributes(mBeanServer, mBeanName);
    for (Map.Entry<String, Map<String, Long>> entry : mBeansMap.entrySet()) {
      assertEquals(
          names, entry.getValue().keySet(), "Mismatched MBean attributes for " + entry.getKey());
    }
  }

  private void assertProducesMessages(final String topicName, final String collectionName) {
    assertProducesMessages(topicName, collectionName, false);
  }

  private void assertProducesMessages(
      final String topicName, final String collectionName, final boolean restartConnector) {
    assertProducesMessages(topicName, collectionName, restartConnector, 1);
  }

  private void assertProducesMessages(
      final String topicName, final String collectionName, final int partitionCount) {
    assertProducesMessages(topicName, collectionName, false, partitionCount);
  }

  private void assertProducesMessages(
      final String topicName,
      final String collectionName,
      final boolean restartConnector,
      final int partitionCount) {

    List<TweetMsg> tweets =
        IntStream.range(0, 100)
            .mapToObj(
                i ->
                    TweetMsg.newBuilder()
                        .setId$1(i)
                        .setText(
                            format(
                                "test tweet %s end2end testing apache kafka <-> mongodb sink connector is fun!",
                                i))
                        .setHashtags(asList(format("t%s", i), "kafka", "mongodb", "testing"))
                        .build())
            .collect(Collectors.toList());

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, topicName);
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
    producerProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroSerializer");
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroSerializer");
    producerProps.put(
        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, KAFKA.schemaRegistryUrl());

    try (KafkaProducer<Long, TweetMsg> producer = new KafkaProducer<>(producerProps)) {
      producer.initTransactions();
      producer.beginTransaction();
      tweets.stream()
          .filter(t -> t.getId$1() < 50)
          .forEach(
              tweet ->
                  producer.send(
                      new ProducerRecord<>(
                          topicName, RANDOM.nextInt(partitionCount), tweet.getId$1(), tweet)));
      producer.commitTransaction();

      assertEventuallyEquals(
          50L, () -> getCollection(collectionName).countDocuments(), collectionName);

      if (restartConnector) {
        restartSinkConnector();
      }

      producer.beginTransaction();
      tweets.stream()
          .filter(t -> t.getId$1() >= 50)
          .forEach(
              tweet ->
                  producer.send(
                      new ProducerRecord<>(
                          topicName, RANDOM.nextInt(partitionCount), tweet.getId$1(), tweet)));
      producer.commitTransaction();
      assertEventuallyEquals(
          100L, () -> getCollection(collectionName).countDocuments(), collectionName);
    }
  }

  <T> void assertEventuallyEquals(final T expected, final Supplier<T> action, final String msg) {
    assertEventuallyEquals(expected, action, msg, 5, 1000);
  }

  <T> void assertEventuallyEquals(
      final T expected,
      final Supplier<T> action,
      final String msg,
      final int retries,
      final long timeoutMs) {
    int counter = 0;
    boolean hasError = true;
    AssertionFailedError exception = null;
    while (counter < retries && hasError) {
      try {
        counter++;
        assertEquals(expected, action.get(), msg);
        hasError = false;
      } catch (AssertionFailedError e) {
        LOGGER.debug("Failed assertion on attempt: {}", counter);
        exception = e;
        try {
          Thread.sleep(timeoutMs);
        } catch (InterruptedException interruptedException) {
          // ignore
        }
      }
    }
    if (hasError && exception != null) {
      throw exception;
    }
  }

  private void assertCollectionOrder(final boolean exact) {
    assertCollectionOrder(getCollectionName(), exact);
  }

  private void assertCollectionOrder(final String collectionName, final boolean exactOrdering) {
    List<Long> expectedIdOrder = LongStream.range(0, 100).boxed().collect(Collectors.toList());
    List<Long> idOrder =
        getCollection(collectionName)
            .find()
            .sort(Sorts.ascending("_id"))
            .into(new ArrayList<>())
            .stream()
            .map(d -> d.getLong("id"))
            .collect(Collectors.toList());

    assertEquals(
        new HashSet<>(expectedIdOrder),
        new HashSet<>(idOrder),
        format("%s missing expected values.", collectionName));
    if (exactOrdering) {
      assertEquals(expectedIdOrder, idOrder, format("%s is out of order.", collectionName));
    } else {
      assertNotEquals(
          expectedIdOrder, idOrder, format("%s unexpectedly in order.", collectionName));
    }
  }
}
