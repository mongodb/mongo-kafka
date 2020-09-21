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
package com.mongodb.kafka.connect.mongodb;

import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.ChangeStreamOperation;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import io.confluent.connect.avro.AvroConverter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.kafka.connect.MongoSinkConnector;
import com.mongodb.kafka.connect.MongoSourceConnector;
import com.mongodb.kafka.connect.embedded.EmbeddedKafka;
import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;

public class MongoKafkaTestCase {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MongoKafkaTestCase.class);
  protected static final AtomicInteger POSTFIX = new AtomicInteger();
  private static final int DEFAULT_MAX_RETRIES = 15;
  private static final int DEFAULT_EMPTY_RETRIES = 5;
  private static final OutputFormat DEFAULT_OUTPUT_FORMAT = OutputFormat.JSON;

  @RegisterExtension public static final EmbeddedKafka KAFKA = new EmbeddedKafka();
  @RegisterExtension public static final MongoDBHelper MONGODB = new MongoDBHelper();

  public String getTopicName() {
    return format("%s%s", getCollectionName(), POSTFIX.incrementAndGet());
  }

  public MongoClient getMongoClient() {
    return MONGODB.getMongoClient();
  }

  public String getDatabaseName() {
    return MONGODB.getDatabaseName();
  }

  public MongoDatabase getDatabase() {
    return MONGODB.getDatabase();
  }

  public String getCollectionName() {
    String collection = MONGODB.getConnectionString().getCollection();
    return collection != null ? collection : getClass().getSimpleName();
  }

  public MongoCollection<Document> getCollection() {
    return getCollection(getCollectionName());
  }

  public MongoCollection<Document> getCollection(final String name) {
    return MONGODB.getDatabase().getCollection(name);
  }

  public boolean isReplicaSetOrSharded() {
    Document isMaster =
        MONGODB
            .getMongoClient()
            .getDatabase("admin")
            .runCommand(BsonDocument.parse("{isMaster: 1}"));
    return isMaster.containsKey("setName") || isMaster.get("msg", "").equals("isdbgrid");
  }

  private static final int THREE_DOT_SIX_WIRE_VERSION = 6;
  private static final int FOUR_DOT_ZERO_WIRE_VERSION = 7;
  private static final int FOUR_DOT_TWO_WIRE_VERSION = 8;

  public boolean isGreaterThanThreeDotSix() {
    return getMaxWireVersion() > THREE_DOT_SIX_WIRE_VERSION;
  }

  public boolean isGreaterThanFourDotZero() {
    return getMaxWireVersion() > FOUR_DOT_ZERO_WIRE_VERSION;
  }

  public boolean isGreaterThanFourDotTwo() {
    return getMaxWireVersion() > FOUR_DOT_TWO_WIRE_VERSION;
  }

  public int getMaxWireVersion() {
    Document isMaster =
        MONGODB
            .getMongoClient()
            .getDatabase("admin")
            .runCommand(BsonDocument.parse("{isMaster: 1}"));
    return isMaster.get("maxWireVersion", 0);
  }

  public void cleanUp() {
    KAFKA.resetOffsets();
    getMongoClient()
        .listDatabaseNames()
        .into(new ArrayList<>())
        .forEach(
            i -> {
              if (i.startsWith(getDatabaseName())) {
                getMongoClient().getDatabase(i).drop();
              }
            });
  }

  public MongoDatabase getDatabaseWithPostfix() {
    return getMongoClient()
        .getDatabase(format("%s%s", getDatabaseName(), POSTFIX.incrementAndGet()));
  }

  public MongoCollection<Document> getAndCreateCollection() {
    MongoDatabase database = getDatabaseWithPostfix();
    database.createCollection("coll");
    return database.getCollection("coll");
  }

  private static final String SIMPLE_DOCUMENT = "{_id: %s}";

  public List<Document> createDocuments(final IntStream stream) {
    return createDocuments(stream, SIMPLE_DOCUMENT);
  }

  public List<Document> createDocuments(final IntStream stream, final String json) {
    return stream.mapToObj(i -> Document.parse(format(json, i))).collect(toList());
  }

  public List<Document> insertMany(
      final IntStream stream, final MongoCollection<?>... collections) {
    return insertMany(stream, SIMPLE_DOCUMENT, collections);
  }

  public List<Document> insertMany(
      final IntStream stream, final String json, final MongoCollection<?>... collections) {
    List<Document> docs = createDocuments(stream, json);
    for (MongoCollection<?> c : collections) {
      LOGGER.debug("Inserting {} documents into {} ", docs.size(), c.getNamespace().getFullName());
      c.withDocumentClass(Document.class).insertMany(docs);
    }
    return docs;
  }

  public <T> void assertCollection(final List<T> expected, final MongoCollection<T> destination) {
    int counter = 0;
    int retryCount = 0;
    while (retryCount < DEFAULT_MAX_RETRIES) {
      counter++;
      // Wait at least 3 minutes for the first data to come in.
      if (counter > 90) {
        retryCount++;
      }
      if (retryCount != DEFAULT_MAX_RETRIES && destination.countDocuments() < expected.size()) {
        sleep(2000);
      } else {
        assertIterableEquals(expected, destination.find().into(new ArrayList<>()));
        break;
      }
    }
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes, final MongoCollection<?> coll) {
    assertProduced(
        operationTypes,
        coll,
        operationTypes.isEmpty() ? DEFAULT_EMPTY_RETRIES : DEFAULT_MAX_RETRIES,
        DEFAULT_OUTPUT_FORMAT);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes,
      final MongoCollection<?> coll,
      final OutputFormat outputFormat) {
    assertProduced(
        operationTypes,
        coll,
        operationTypes.isEmpty() ? DEFAULT_EMPTY_RETRIES : DEFAULT_MAX_RETRIES,
        outputFormat);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes,
      final MongoCollection<?> coll,
      final int maxRetryCount,
      final OutputFormat outputFormat) {
    assertProduced(operationTypes, coll.getNamespace().getFullName(), maxRetryCount, outputFormat);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes, final String topicName) {
    assertProduced(operationTypes, topicName, DEFAULT_MAX_RETRIES);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes,
      final String topicName,
      final int maxRetryCount) {
    assertProduced(operationTypes, topicName, maxRetryCount, DEFAULT_OUTPUT_FORMAT);
  }

  public void assertProduced(
      final List<ChangeStreamOperation> operationTypes,
      final String topicName,
      final int maxRetryCount,
      final OutputFormat outputFormat) {

    List<ChangeStreamOperation> produced;
    switch (outputFormat) {
      case JSON:
        produced =
            getProduced(
                topicName,
                ChangeStreamOperations::createChangeStreamOperationJson,
                operationTypes,
                maxRetryCount);
        break;
      case BSON:
        produced =
            getProduced(
                topicName,
                ChangeStreamOperations::createChangeStreamOperationBson,
                operationTypes,
                maxRetryCount);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + outputFormat);
    }
    assertIterableEquals(operationTypes, produced);
  }

  public void assertProducedDocs(final List<Document> docs, final MongoCollection<?> coll) {
    List<Document> produced =
        getProduced(
            coll.getNamespace().getFullName(),
            b -> Document.parse(b.toString()),
            docs,
            DEFAULT_MAX_RETRIES);
    assertIterableEquals(docs, produced);
  }

  private static final Deserializer<Bytes> BYTES_DESERIALIZER = new BytesDeserializer();

  public List<String> getProducedStrings(final String topicName, final int expectedSize) {
    return getProduced(
        topicName,
        BYTES_DESERIALIZER,
        new MappingDeserializer<>(Bytes::toString),
        ConsumerRecord::value,
        expectedSize,
        1);
  }

  public <T> List<T> getProduced(
      final String topicName,
      final Function<Bytes, T> mapper,
      final List<T> expected,
      final int maxRetryCount) {
    return getProduced(
        topicName,
        BYTES_DESERIALIZER,
        new MappingDeserializer<>(mapper),
        ConsumerRecord::value,
        expected,
        maxRetryCount);
  }

  public List<String> getProducedKeys(final MongoCollection<?> collection, final int expectedSize) {
    return getProduced(
        collection.getNamespace().getFullName(),
        new MappingDeserializer<>(Bytes::toString),
        BYTES_DESERIALIZER,
        ConsumerRecord::key,
        expectedSize,
        DEFAULT_MAX_RETRIES);
  }

  public static class MappingDeserializer<T> implements Deserializer<T> {
    private final Function<Bytes, T> mapper;

    public MappingDeserializer(final Function<Bytes, T> mapper) {
      this.mapper = mapper;
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
      return mapper.apply(BYTES_DESERIALIZER.deserialize(topic, data));
    }
  }

  public <K, V, T> List<T> getProduced(
      final String topicName,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer,
      final Function<ConsumerRecord<K, V>, T> mapper,
      final int expectedSize,
      final int maxRetryCount) {
    return getProduced(
        topicName,
        keyDeserializer,
        valueDeserializer,
        mapper,
        emptyList(),
        expectedSize,
        maxRetryCount);
  }

  public <K, V, T> List<T> getProduced(
      final String topicName,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer,
      final Function<ConsumerRecord<K, V>, T> mapper,
      final List<T> expected,
      final int maxRetryCount) {
    return getProduced(
        topicName,
        keyDeserializer,
        valueDeserializer,
        mapper,
        expected,
        expected.size(),
        maxRetryCount);
  }

  public <K, V, T> List<T> getProduced(
      final String topicName,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer,
      final Function<ConsumerRecord<K, V>, T> mapper,
      final List<T> expected,
      final int expectedSize,
      final int maxRetryCount) {
    LOGGER.info("Subscribing to {}", topicName);
    if (!expected.isEmpty() && expected.size() != expectedSize) {
      throw new IllegalArgumentException("Expected list size different from expected size");
    }

    try (KafkaConsumer<K, V> consumer = createConsumer(keyDeserializer, valueDeserializer)) {
      consumer.subscribe(singletonList(topicName));
      List<T> data = new ArrayList<>();
      int counter = 0;
      int retryCount = 0;
      int previousDataSize;

      while (retryCount < maxRetryCount) {
        counter++;
        previousDataSize = data.size();
        consumer
            .poll(Duration.ofSeconds(2))
            .records(topicName)
            .forEach(c -> data.add(mapper.apply(c)));

        if (data.size() >= expectedSize) {
          int startIndex = expected.isEmpty() ? 0 : Collections.indexOfSubList(data, expected);
          if (startIndex > -1) {
            return data.subList(startIndex, startIndex + expectedSize);
          }
        }

        // Wait at least 3 minutes for the first set of data to arrive
        if (expectedSize == 0 || data.size() > 0 || counter > 90) {
          retryCount += previousDataSize == data.size() ? 1 : 0;
        }
      }

      return data;
    }
  }

  public KafkaConsumer<?, ?> createConsumer() {
    return createConsumer(null, null);
  }

  public <K, V> KafkaConsumer<K, V> createConsumer(
      final Deserializer<K> keyDeserializer, final Deserializer<V> valueDeserializer) {
    Properties props = new Properties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "testAssertProducedConsumer");
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.BytesDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.BytesDeserializer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    return new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
  }

  public void addSinkConnector(final String topicName) {
    Properties props = new Properties();
    props.put("topics", topicName);
    addSinkConnector(props);
  }

  public void addSinkConnector(final Properties overrides) {
    Properties props = new Properties();
    props.put("connector.class", MongoSinkConnector.class.getName());
    props.put(MongoSinkConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
    props.put(MongoSinkTopicConfig.DATABASE_CONFIG, MONGODB.getDatabaseName());
    props.put(MongoSinkTopicConfig.COLLECTION_CONFIG, getCollectionName());
    props.put("key.converter", AvroConverter.class.getName());
    props.put("key.converter.schema.registry.url", KAFKA.schemaRegistryUrl());
    props.put("value.converter", AvroConverter.class.getName());
    props.put("value.converter.schema.registry.url", KAFKA.schemaRegistryUrl());

    overrides.forEach(props::put);
    KAFKA.addSinkConnector(props);
  }

  public void addSourceConnector() {
    addSourceConnector(new Properties());
  }

  public void addSourceConnector(final Properties overrides) {
    Properties props = new Properties();
    props.put("connector.class", MongoSourceConnector.class.getName());
    props.put(MongoSourceConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());

    overrides.forEach(props::put);
    KAFKA.addSourceConnector(props);
  }

  public void restartSinkConnector() {
    KAFKA.restartSinkConnector();
  }

  public void restartSourceConnector() {
    KAFKA.restartSourceConnector();
  }

  public void stopStartSourceConnector(final Properties properties) {
    KAFKA.deleteSourceConnector();
    addSourceConnector(properties);
  }

  public void sleep() {
    sleep(2000);
  }

  public void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      // Ignore
    }
  }
}
