/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.end2end;

import at.grahsl.kafka.connect.mongodb.data.avro.TweetMsg;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import okhttp3.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@RunWith(JUnitPlatform.class)
public class MinimumViableIT {

    public static final String DOCKER_COMPOSE_FILE = "src/test/resources/docker/compose-env.yml";
    public static final String DEFAULT_COMPOSE_SERVICE_SUFFIX = "_1";

    public static final String SINK_CONNECTOR_CONFIG = "src/test/resources/config/sink_connector.json";

    public static final String KAFKA_BROKER;
    public static final int KAFKA_BROKER_PORT;

    public static final String KAFKA_CONNECT;
    public static final int KAFKA_CONNECT_PORT;

    public static final String SCHEMA_REGISTRY;
    public static final int SCHEMA_REGISTRY_PORT;

    public static final String MONGODB;
    public static int MONGODB_PORT;

    public static long PROCESSING_DELAY_TIME_MILLIS = 5_000L;

    private static MongoClientURI MONGODB_CLIENT_URI;
    private static MongoClient MONGO_CLIENT;
    private static MongoDatabase MONGO_DATABASE;

    private static KafkaProducer<String, TweetMsg> PRODUCER;

    static {
        try {
            Map composeFile = (Map)new YamlReader(new FileReader(DOCKER_COMPOSE_FILE)).read();

            KAFKA_BROKER = extractHostnameFromDockerCompose(composeFile,"kafkabroker");
            KAFKA_BROKER_PORT = extractHostPortFromDockerCompose(composeFile,"kafkabroker");

            KAFKA_CONNECT = extractHostnameFromDockerCompose(composeFile,"kafkaconnect");
            KAFKA_CONNECT_PORT = extractHostPortFromDockerCompose(composeFile,"kafkaconnect");

            SCHEMA_REGISTRY = extractHostnameFromDockerCompose(composeFile,"schemaregistry");
            SCHEMA_REGISTRY_PORT = extractHostPortFromDockerCompose(composeFile,"schemaregistry");

            MONGODB = extractHostnameFromDockerCompose(composeFile,"mongodb");
            MONGODB_PORT = extractHostPortFromDockerCompose(composeFile,"mongodb");
        } catch(Exception exc) {
            throw new RuntimeException("error: parsing the docker-compose YAML",exc);
        }
    }

    @ClassRule
    public static DockerComposeContainer CONTAINER_ENV =
            new DockerComposeContainer(new File(DOCKER_COMPOSE_FILE))
                    .withExposedService(KAFKA_BROKER+DEFAULT_COMPOSE_SERVICE_SUFFIX,KAFKA_BROKER_PORT)
                    .withExposedService(KAFKA_CONNECT+DEFAULT_COMPOSE_SERVICE_SUFFIX,KAFKA_CONNECT_PORT)
                    .withExposedService(SCHEMA_REGISTRY +DEFAULT_COMPOSE_SERVICE_SUFFIX, SCHEMA_REGISTRY_PORT)
                    .withExposedService(MONGODB+DEFAULT_COMPOSE_SERVICE_SUFFIX,MONGODB_PORT)
            ;

    @BeforeAll
    public static void setup() throws IOException {
        CONTAINER_ENV.starting(Description.EMPTY);
        MONGODB_CLIENT_URI = new MongoClientURI(
                "mongodb://"+ MONGODB+":"+MONGODB_PORT+"/kafkaconnect"
        );
        MONGO_CLIENT = new MongoClient(MONGODB_CLIENT_URI);
        MONGO_DATABASE = MONGO_CLIENT.getDatabase(MONGODB_CLIENT_URI.getDatabase());

        Properties props = new Properties();
        props.put("bootstrap.servers",KAFKA_BROKER+":"+KAFKA_BROKER_PORT);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url","http://"+SCHEMA_REGISTRY+":"+ SCHEMA_REGISTRY_PORT);
        PRODUCER = new KafkaProducer<>(props);

        String config = new String(Files.readAllBytes(Paths.get(SINK_CONNECTOR_CONFIG)));
        registerMongoDBSinkConnector(config);
    }

    @Test
    @DisplayName("when producing kafka records then verify saved mongodb documents")
    public void produceKafkaRecordsAndVerifyMongoDbDocuments() {

        int numTestRecords = 100;
        for (int tid = 0; tid < numTestRecords; tid++) {
            TweetMsg tweet = TweetMsg.newBuilder()
                    .setId$1(tid)
                    .setText("test tweet " + (tid) + ": end2end testing apache kafka <-> mongodb sink connector is fun!")
                    .setHashtags(Arrays.asList(new String[]{"t" + tid, "kafka", "mongodb", "testing"}))
                    .build();

            ProducerRecord<String, TweetMsg> record = new ProducerRecord<>("e2e-test-topic", tweet);
            System.out.println(LocalDateTime.now() + " producer sending -> " + tweet.toString());

            PRODUCER.send(record, (RecordMetadata r, Exception exc) -> {
                assertNull(exc, () -> "unexpected error while sending: " + tweet
                        + " | exc: " + exc.getMessage()
                );
            });
        }

        deferExecutionToWaitForDataPropagation(PROCESSING_DELAY_TIME_MILLIS);

        for (int tid = 0; tid < numTestRecords; tid++) {
            MongoCollection<Document> col = MONGO_DATABASE.getCollection("e2e-test-collection");
            Document found = col.find(eq(tid)).first();
            //NOTE: this only verifies whether a document with the expected _id field
            //exists everything else isn't particularly interesting during E2E scenarios
            //but instead rigorously checked within a bunch of specific unit tests
            assertNotNull(found, "document having _id=" + tid + " is expected to be found");
            System.out.println("found = " + found);
        }

    }

    private static void registerMongoDBSinkConnector(String configuration) throws IOException {
        RequestBody body = RequestBody.create(
                MediaType.parse("application/json"), configuration
        );

        Request request = new Request.Builder()
                .url("http://"+KAFKA_CONNECT+":"+KAFKA_CONNECT_PORT+"/connectors")
                .post(body)
                .build();

        System.out.println("sending -> "+configuration);
        Response response = new OkHttpClient().newCall(request).execute();
        assert(response.code() == 201);
        response.close();
    }

    private static void deferExecutionToWaitForDataPropagation(long millis) {
        System.out.println("giving the processing some time to propagate the data...");
        try {
            Thread.sleep(PROCESSING_DELAY_TIME_MILLIS);
        } catch (InterruptedException e) {}
    }

    private static String extractHostnameFromDockerCompose(Map compose,String serviceName) {
        return (String)((Map)((Map)compose.get("services")).get(serviceName)).get("hostname");
    }

    private static int extractHostPortFromDockerCompose(Map compose,String serviceName) {
        return Integer.parseInt(((String)((List)((Map)((Map)compose.get("services"))
                .get(serviceName)).get("ports")).get(0)).split(":")[1]);
    }

}
