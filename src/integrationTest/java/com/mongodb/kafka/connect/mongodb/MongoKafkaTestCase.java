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
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createChangeStreamOperation;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.confluent.connect.avro.AvroConverter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy;
import com.mongodb.kafka.connect.source.MongoSourceConfig;

public class MongoKafkaTestCase {
    protected static final Logger LOGGER = LoggerFactory.getLogger(MongoKafkaTestCase.class);
    protected static final AtomicInteger POSTFIX = new AtomicInteger();

    @RegisterExtension
    public static final EmbeddedKafka KAFKA = new EmbeddedKafka();
    @RegisterExtension
    public static final MongoDBHelper MONGODB = new MongoDBHelper();

    public String getTopicName() {
        return  format("%s%s",  getCollection().getNamespace().getFullName(), POSTFIX.incrementAndGet());
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
        Document isMaster = MONGODB.getMongoClient().getDatabase("admin").runCommand(BsonDocument.parse("{isMaster: 1}"));
        return isMaster.containsKey("setName") || isMaster.get("msg", "").equals("isdbgrid");
    }

    public void assertProduced(final int expectedCount, final String topicName) {
        assertEquals(expectedCount, getProduced(expectedCount, topicName).size());
    }

    public void assertProduced(final List<ChangeStreamOperation> operationTypes, final MongoCollection<?> coll) {
        assertProduced(operationTypes, coll.getNamespace().getFullName());
    }

    public void assertProduced(final List<ChangeStreamOperation> operationTypes, final String topicName) {
        List<ChangeStreamOperation> produced = getProduced(operationTypes.size(), topicName).stream()
                .map((b)-> createChangeStreamOperation(b.toString())).collect(Collectors.toList());
        assertIterableEquals(operationTypes, produced);
    }

    public void assertProducedDocs(final List<Document> docs, final MongoCollection<?> coll) {
        assertEquals(docs, getProduced(docs.size(), coll.getNamespace().getFullName()).stream()
                .map((b)-> Document.parse(b.toString())).collect(Collectors.toList()));
    }

    public List<Bytes> getProduced(final int expectedCount, final String topicName) {
        LOGGER.info("Subscribing to {} expecting to see #{}", topicName, expectedCount);
        try (KafkaConsumer<?, ?> consumer = createConsumer()) {
            consumer.subscribe(singletonList(topicName));
            List<Bytes> data = new ArrayList<>();
            int retryCount = 0;
            while (data.size() < expectedCount && retryCount < 30) {
                consumer.poll(Duration.ofSeconds(2)).records(topicName).forEach((r) -> data.add((Bytes) r.value()));
                retryCount++;
                LOGGER.info("Polling {} ({}) seen: #{}", topicName, retryCount, data.size());
            }
            return data;
        }
    }

    public KafkaConsumer<?, ?> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testAssertProducedConsumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new KafkaConsumer<>(props);
    }

    public void addSinkConnector(final String topicName) {
        Properties props = new Properties();
        props.put("topics", topicName);
        props.put("connector.class", MongoSinkConnector.class.getName());
        props.put(MongoSinkConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
        props.put(MongoSinkTopicConfig.DATABASE_CONFIG, MONGODB.getDatabaseName());
        props.put(MongoSinkTopicConfig.COLLECTION_CONFIG, getCollectionName());
        props.put(MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG, ProvidedInValueStrategy.class.getName());
        props.put("key.converter", AvroConverter.class.getName());
        props.put("key.converter.schema.registry.url", KAFKA.schemaRegistryUrl());
        props.put("value.converter", AvroConverter.class.getName());
        props.put("value.converter.schema.registry.url", KAFKA.schemaRegistryUrl());

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
        sleep(10000);
    }
    public void restartConnector() {
        restartConnector(new Properties());
    }

    public void restartConnector(final Properties overrides) {
        KAFKA.deleteSourceConnector();
        sleep(5000);
        addSourceConnector(overrides);
    }

}
