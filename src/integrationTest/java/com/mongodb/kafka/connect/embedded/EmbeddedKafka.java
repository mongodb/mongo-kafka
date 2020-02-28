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

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.streams.integration.utils.KafkaEmbedded;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import kafka.server.KafkaConfig$;
import kafka.utils.MockTime;
import kafka.zk.KafkaZkClient;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance, 1 Kafka broker, 1
 * Confluent Schema Registry instance, and 1 Confluent Connect instance.
 */
public class EmbeddedKafka implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKafka.class);
    private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected
    private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
    private static final String AVRO_COMPATIBILITY_TYPE = AvroCompatibilityLevel.NONE.name;

    private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "10000";
    private static final String KAFKASTORE_DEBUG = "true";
    private static final String KAFKASTORE_INIT_TIMEOUT = "90000";

    private static final String SINK_CONNECTOR_NAME = "MongoSinkConnector";
    private static final String SOURCE_CONNECTOR_NAME = "MongoSourceConnector";
    private final Properties brokerConfig;
    private ZooKeeperEmbedded zookeeper;
    private KafkaZkClient zkClient = null;
    private KafkaEmbedded broker;
    private ConnectStandalone connect;
    private RestApp schemaRegistry;
    private boolean running;
    private boolean addedSink;
    private boolean addedSource;
    private List<String> topics = new ArrayList<>();

    /**
     * Creates and starts the cluster.
     */
    public EmbeddedKafka() {
        this(new Properties());
    }

    /**
     * Creates and starts the cluster.
     *
     * @param brokerConfig Additional broker configuration settings.
     */
    public EmbeddedKafka(final Properties brokerConfig) {
        Properties brokerProps = new Properties();
        brokerProps.putAll(brokerConfig);

        // If log.dir is not set.
        if (brokerProps.getProperty("log.dir") == null) {
            // Create temp path to store logs and set property.
            brokerProps.setProperty("log.dir", createTempDirectory().getAbsolutePath());
        }

        // Ensure that we're advertising correct hostname appropriately
        brokerProps.setProperty("host.name", brokerProps.getProperty("host.name", "localhost"));

        brokerProps.setProperty("auto.create.topics.enable", brokerProps.getProperty("auto.create.topics.enable", "true"));
        brokerProps.setProperty("zookeeper.session.timeout.ms", brokerProps.getProperty("zookeeper.session.timeout.ms", "30000"));
        brokerProps.setProperty("broker.id", brokerProps.getProperty("broker.id", "1"));
        brokerProps.setProperty("auto.offset.reset", brokerProps.getProperty("auto.offset.reset", "latest"));

        // Lower active threads.
        brokerProps.setProperty("num.io.threads", brokerProps.getProperty("num.io.threads", "2"));
        brokerProps.setProperty("num.network.threads", brokerProps.getProperty("num.network.threads", "2"));
        brokerProps.setProperty("log.flush.interval.messages", brokerProps.getProperty("log.flush.interval.messages", "1"));

        // Define replication factor for internal topics to 1
        brokerProps.setProperty("offsets.topic.replication.factor", brokerProps.getProperty("offsets.topic.replication.factor", "1"));
        brokerProps.setProperty("offset.storage.replication.factor", brokerProps.getProperty("offset.storage.replication.factor", "1"));
        brokerProps.setProperty("transaction.state.log.replication.factor", brokerProps.getProperty("transaction.state.log.replication.factor", "1"));
        brokerProps.setProperty("transaction.state.log.min.isr", brokerProps.getProperty("transaction.state.log.min.isr", "1"));
        brokerProps.setProperty("transaction.state.log.num.partitions", brokerProps.getProperty("transaction.state.log.num.partitions", "4"));
        brokerProps.setProperty("config.storage.replication.factor", brokerProps.getProperty("config.storage.replication.factor", "1"));
        brokerProps.setProperty("status.storage.replication.factor", brokerProps.getProperty("status.storage.replication.factor", "1"));
        brokerProps.setProperty("default.replication.factor", brokerProps.getProperty("default.replication.factor", "1"));

        this.brokerConfig = brokerProps;
    }

    private static File createTempDirectory() {
        // Create temp path to store logs
        final File logDir = Files.createTempDir();

        // Ensure its removed on termination.
        logDir.deleteOnExit();
        return logDir;
    }

    /**
     * Creates and starts the cluster.
     */
    public void start() throws Exception {
        LOGGER.debug("Initiating embedded Kafka cluster startup");
        LOGGER.debug("Starting a ZooKeeper instance...");
        zookeeper = new ZooKeeperEmbedded();
        LOGGER.debug("ZooKeeper instance is running at {}", zookeeper.connectString());


        zkClient = KafkaZkClient.apply(zookeeper.connectString(), JaasUtils.isZkSecurityEnabled(), 30000, 30000, 1000,
                new MockTime(), "kafka.server", "SessionExpireListener");


        final Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
        LOGGER.debug("Starting a Kafka instance on port {} ...", effectiveBrokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
        broker = new KafkaEmbedded(effectiveBrokerConfig, new MockTime());
        LOGGER.debug("Kafka instance is running at {}, connected to ZooKeeper at {}", broker.brokerList(), broker.zookeeperConnect());

        final Properties schemaRegistryProps = new Properties();
        schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
        schemaRegistryProps.put(SchemaRegistryConfig.DEBUG_CONFIG, KAFKASTORE_DEBUG);
        schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG, KAFKASTORE_INIT_TIMEOUT);

        schemaRegistry = new RestApp(0, zookeeperConnect(), KAFKA_SCHEMAS_TOPIC, AVRO_COMPATIBILITY_TYPE, schemaRegistryProps);
        schemaRegistry.start();

        LOGGER.debug("Starting a Connect standalone instance...");
        connect = new ConnectStandalone(connectWorkerConfig());
        connect.start();
        LOGGER.debug("Connect standalone instance is running at {}", connect.getConnectionString());
        running = true;
    }

    public void addSinkConnector(final Properties properties) {
        properties.put("name", SINK_CONNECTOR_NAME);
        LOGGER.info("Adding connector: {}", properties);
        connect.addConnector(SINK_CONNECTOR_NAME, properties);
        addedSink = true;
    }

    public void deleteSinkConnector() {
        if (addedSink) {
            connect.deleteConnector(SINK_CONNECTOR_NAME);
            addedSink = false;
        }
    }

    public void addSourceConnector(final Properties properties) {
        properties.put("name", SOURCE_CONNECTOR_NAME);
        LOGGER.info("Adding connector: {}", properties);
        connect.addConnector(SOURCE_CONNECTOR_NAME, properties);
        addedSource = true;
    }

    public void deleteSourceConnector() {
        if (addedSource) {
            connect.deleteConnector(SOURCE_CONNECTOR_NAME);
            addedSource = false;
        }
    }

    private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final ZooKeeperEmbedded zookeeper) {
        final Properties effectiveConfig = new Properties();
        effectiveConfig.putAll(brokerConfig);
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 30 * 1000);
        effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), 60 * 1000);
        effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
        effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
        effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        return effectiveConfig;
    }

    private Properties connectWorkerConfig() {
        Properties workerProps = new Properties();
        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, "mongo-kafka-test");
        workerProps.put(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
        workerProps.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.put("value.converter.schemas.enable", "false");
        workerProps.put(DistributedConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "100");
        workerProps.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, createTempDirectory().getAbsolutePath() + "connect");

        return workerProps;
    }

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        start();
    }

    @Override
    public void afterEach(final ExtensionContext context) throws InterruptedException {
        deleteTopicsAndWait(Duration.ofMinutes(4));
        deleteSinkConnector();
        deleteSourceConnector();
    }

    @Override
    public void afterAll(final ExtensionContext context) {
        stop();
    }

    /**
     * Stops the cluster.
     */
    public void stop() {
        LOGGER.info("Stopping Confluent");
        try {
            if (connect != null) {
                connect.stop();
            }
            try {
                if (schemaRegistry != null) {
                    schemaRegistry.stop();
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            if (broker != null) {
                broker.stop();
            }
            try {
                if (zookeeper != null) {
                    zookeeper.stop();
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } finally {
            running = false;
        }
        LOGGER.info("Confluent Stopped");
    }

    /**
     * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
     *
     * You can use this to tell Kafka Streams applications, Kafka producers, and Kafka consumers (new
     * consumer API) how to connect to this cluster.
     */
    public String bootstrapServers() {
        return broker.brokerList();
    }

    /**
     * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
     * Example: `127.0.0.1:2181`.
     *
     * You can use this to e.g. tell Kafka consumers (old consumer API) how to connect to this
     * cluster.
     */
    public String zookeeperConnect() {
        return zookeeper.connectString();
    }

    /**
     * The "schema.registry.url" setting of the schema registry instance.
     */
    public String schemaRegistryUrl() {
        return schemaRegistry.restConnect;
    }

    /**
     * Creates a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(final String topic) {
        createTopic(topic, 1, 1, new Properties());
    }

    /**
     * Creates a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(final String topic, final int partitions, final int replication) {
        createTopic(topic, partitions, replication, new Properties());
    }

    /**
     * Creates a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (partitions of) this topic.
     * @param topicConfig Additional topic-level configuration settings.
     */
    public void createTopic(final String topic, final int partitions, final int replication, final Properties topicConfig) {
        topics.add(topic);
        broker.createTopic(topic, partitions, replication, topicConfig);
    }

    /**
     * Deletes multiple topics and blocks until all topics got deleted.
     *
     * @param topics the name of the topics
     */
    public void deleteTopicsAndWait(final String... topics) throws InterruptedException {
        deleteTopicsAndWait(Duration.ofSeconds(-1), topics);
    }

    /**
     * Delete all topics
     *
     * @param duration the max time to wait for the topics to be deleted
     * @throws InterruptedException
     */
    public void deleteTopicsAndWait(final Duration duration) throws InterruptedException {
        String[] topicArray = topics.toArray(new String[0]);
        topics = new ArrayList<>();
        deleteTopicsAndWait(duration, topicArray);
    }

    /**
     * Deletes multiple topics and blocks until all topics got deleted.
     *
     * @param duration the max time to wait for the topics to be deleted (does not block if {@code <= 0})
     * @param topics   the name of the topics
     */
    public void deleteTopicsAndWait(final Duration duration, final String... topics) throws InterruptedException {
        for (final String topic : topics) {
            try {
                broker.deleteTopic(topic);
            } catch (final UnknownTopicOrPartitionException e) {
            }
        }

        if (!duration.isNegative()) {
            TestUtils.waitForCondition(new TopicsDeletedCondition(topics), duration.toMillis(),
                    format("Topics not deleted after %s milli seconds.", duration.toMillis()));
        }
    }

    public boolean isRunning() {
        return running;
    }

    private final class TopicsDeletedCondition implements TestCondition {
        final Set<String> deletedTopics = new HashSet<>();

        private TopicsDeletedCondition(final String... topics) {
            Collections.addAll(deletedTopics, topics);
        }

        @Override
        public boolean conditionMet() {
            final Set<String> allTopics = new HashSet<>(scala.collection.JavaConversions.seqAsJavaList(zkClient.getAllTopicsInCluster()));
            return !allTopics.removeAll(deletedTopics);
        }
    }

}
