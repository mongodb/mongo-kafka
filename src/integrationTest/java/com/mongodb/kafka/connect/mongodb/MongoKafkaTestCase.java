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

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import com.mongodb.kafka.connect.MongoSinkConnectorConfig;
import com.mongodb.kafka.connect.embedded.EmbeddedKafka;

public class MongoKafkaTestCase {
    @RegisterExtension
    public static final EmbeddedKafka KAFKA = new EmbeddedKafka();
    @RegisterExtension
    public static final MongoDBHelper MONGODB = new MongoDBHelper();

    private static final AtomicInteger COUNTER = new AtomicInteger();
    private final AtomicReference<String> topicName = new AtomicReference<>(getCollectionName());

    @BeforeEach
    public void beforeEach() {
        getCollection().drop();
        createNewTopicName();
    }

    @AfterEach
    public void afterEach() throws InterruptedException {
        getCollection().drop();
        KAFKA.deleteTopicsAndWait(Duration.ofSeconds(-1), getTopicName());
        KAFKA.deleteSinkConnector();
    }

    public String getTopicName() {
        return topicName.get();
    }

    private void createNewTopicName() {
        topicName.set(format("%s-T%s", getCollectionName(), COUNTER.getAndIncrement()));
    }

    public String getCollectionName() {
        String collection = MONGODB.getConnectionString().getCollection();
        return collection != null ? collection : getClass().getSimpleName();
    }

    public MongoCollection<Document> getCollection() {
        return MONGODB.getDatabase().getCollection(getCollectionName());
    }

    public void assertProduced(final int expectedCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, getTopicName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<Bytes, Bytes> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(getTopicName()));
        ConsumerRecords<Bytes, Bytes> records = consumer.poll(Duration.ofMinutes(2));
        consumer.close();
        assertEquals(expectedCount, records.count());
    }

    public void addSinkConnector() {
        addSinkConnector(new Properties());
    }

    public void addSinkConnector(final Properties overrides) {
        Properties props = new Properties();
        props.put("topics", getTopicName());
        props.put("connector.class", "com.mongodb.kafka.connect.MongoSinkConnector");
        props.put(MongoSinkConnectorConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
        props.put(MongoSinkConnectorConfig.DATABASE_NAME_CONFIG, MONGODB.getDatabaseName());
        props.put(MongoSinkConnectorConfig.DOCUMENT_ID_STRATEGIES_CONFIG, "com.mongodb.kafka.connect.processor.id.strategy.ProvidedInValueStrategy");
        props.put(MongoSinkConnectorConfig.COLLECTION_CONFIG, getCollectionName());
        props.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        props.put("key.converter.schema.registry.url", KAFKA.schemaRegistryUrl());
        props.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        props.put("value.converter.schema.registry.url", KAFKA.schemaRegistryUrl());

        overrides.forEach(props::put);
        KAFKA.addSinkConnector(props);
    }
}
