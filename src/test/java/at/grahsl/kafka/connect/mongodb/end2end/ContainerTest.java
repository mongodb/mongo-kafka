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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@RunWith(JUnitPlatform.class)
public class ContainerTest {

    public static final String DOCKER_COMPOSE_FILE = "src/test/resources/docker/compose-env.yml";
    public static final String DEFAULT_COMPOSE_SERVICE_SUFFIX = "_1";

    public static final String SINK_CONNECTOR_CONFIG = "src/test/resources/config/sink_connector.json";

    //TODO: read these host:port settings from .yml compose file

    public static final String KAFKA_BROKER = "kafkabroker";
    public static final int KAFKA_BROKER_PORT = 9092;

    public static final String KAFKA_CONNECT = "kafkaconnect";
    public static final int KAFKA_CONNECT_PORT = 8083;

    public static final String SCHEMA_REGISTRY = "schemaregistry";
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public static final String MONGODB = "mongodb";
    public static int MONGODB_PORT = 27017;

    public static long PROCESSING_DELAY_TIME_MILLIS = 5_000L;

    private static MongoClientURI MONGODB_CLIENT_URI;
    private static MongoClient MONGO_CLIENT;
    private static MongoDatabase MONGO_DATABASE;

    private static KafkaProducer<String, TweetMsg> PRODUCER;

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

    @RepeatedTest(value=100, name = "{displayName} {currentRepetition}/{totalRepetitions}")
    @DisplayName("producing avro records to kafka topic")
    public void produceKafkaAvroRecords(RepetitionInfo info) {

        int tweetCnt = info.getCurrentRepetition();

        TweetMsg tweet = TweetMsg.newBuilder()
                .setId$1(tweetCnt)
                .setText("test tweet "+(tweetCnt)+": end2end testing apache kafka <-> mongodb sink connector is fun!")
                .setHashtags(Arrays.asList(new String[]{"t"+tweetCnt,"kafka","mongodb","testing"}))
                .build();

        ProducerRecord<String, TweetMsg> record = new ProducerRecord<>("e2e-test-topic", tweet);
        System.out.println(LocalDateTime.now() + " producer sending -> " + tweet.toString());

        PRODUCER.send(record, (RecordMetadata r, Exception exc) -> {
            assertNull(exc, () -> "unexpected error while sending: " + tweet
                                        + " | exc: "+exc.getMessage()
            );
        });

    }

    @RepeatedTest(value = 100, name = "{displayName} {currentRepetition}/{totalRepetitions}")
    @DisplayName("looking up expected records in MongoDB collection")
    public void verifyMongoDbSinkRecords(RepetitionInfo info) {

        //defer manually wait for data propagation before first read back
        if(info.getCurrentRepetition() == 1) {
            deferExecutionToWaitForDataPropagation(PROCESSING_DELAY_TIME_MILLIS);
        }

        int tweetCnt = info.getCurrentRepetition();
        MongoCollection<Document> col = MONGO_DATABASE.getCollection("e2e-test-collection");
        List<Document> docs = new ArrayList<>();

        Document found = col.find(eq(tweetCnt)).first();
        assertNotNull(found, "document having _id="+tweetCnt+" is expected to be found");
        System.out.println("found = " + found);

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

}
