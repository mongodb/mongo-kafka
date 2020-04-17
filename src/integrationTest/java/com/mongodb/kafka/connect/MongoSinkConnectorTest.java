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

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_REGEX_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPIC_OVERRIDE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.COLLECTION_CONFIG;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.kafka.connect.avro.TweetMsg;
import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;

class MongoSinkConnectorTest extends MongoKafkaTestCase {

    @Test
    @DisplayName("Ensure sink connect saves data to MongoDB")
    void testSinkSavesAvroDataToMongoDB() {
        String topicName = getTopicName();
        KAFKA.createTopic(topicName);
        addSinkConnector(topicName);

        assertProducesMessages(topicName, getCollectionName());
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

        Properties sinkProperties = new Properties();
        sinkProperties.put(TOPICS_REGEX_CONFIG, "topic\\-regex\\-(.*)");
        sinkProperties.put(format(TOPIC_OVERRIDE_CONFIG, topicName1, COLLECTION_CONFIG), collectionName1);
        sinkProperties.put(format(TOPIC_OVERRIDE_CONFIG, topicName2, COLLECTION_CONFIG), collectionName2);
        addSinkConnector(sinkProperties);

        assertProducesMessages(topicName1, collectionName1);
        assertProducesMessages(topicName2, collectionName2);
    }

    @Test
    @DisplayName("Ensure sink can survive a restart")
    void testSinkSurvivesARestart() {
        String topicName = getTopicName();
        KAFKA.createTopic(topicName);
        addSinkConnector(topicName);
        assertProducesMessages(topicName, getCollectionName(), true);
    }

    private void assertProducesMessages(final String topicName, final String collectionName) {
        assertProducesMessages(topicName, collectionName, false);
    }

    private void assertProducesMessages(final String topicName, final String collectionName, final boolean restartConnector) {

        List<TweetMsg> tweets = IntStream.range(0, 100).mapToObj(i ->
                TweetMsg.newBuilder().setId$1(i)
                        .setText(format("test tweet %s end2end testing apache kafka <-> mongodb sink connector is fun!", i))
                        .setHashtags(asList(format("t%s", i), "kafka", "mongodb", "testing"))
                        .build()
        ).collect(Collectors.toList());

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, topicName);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, KAFKA.schemaRegistryUrl());

        try (KafkaProducer<String, TweetMsg> producer = new KafkaProducer<>(producerProps)) {
            producer.initTransactions();
            producer.beginTransaction();
            tweets.stream().filter(t -> t.getId$1() < 50).forEach(tweet -> producer.send(new ProducerRecord<>(topicName, tweet)));
            producer.commitTransaction();

            assertProduced(50, topicName);
            assertEquals(50, getCollection(collectionName).countDocuments(), collectionName);

            if (restartConnector) {
                restartSinkConnector(topicName);
            }

            producer.beginTransaction();
            tweets.stream().filter(t -> t.getId$1() >= 50).forEach(tweet -> producer.send(new ProducerRecord<>(topicName, tweet)));
            producer.commitTransaction();

            assertProduced(100, topicName);
            assertEquals(100, getCollection(collectionName).countDocuments());
        }
    }
}
