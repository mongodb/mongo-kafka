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

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPIC_OVERRIDE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.FULL_DOCUMENT_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_PREFIX_CONFIG;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.storage.StringConverter;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler;

public class ChangeStreamRoundTripTest extends MongoKafkaTestCase {

  @BeforeEach
  void setUp() {
    assumeTrue(isReplicaSetOrSharded());
    cleanUp();
  }

  @AfterEach
  void tearDown() {
    cleanUp();
  }

  @Test
  @DisplayName("Ensure collection CRUD operations can be round tripped")
  void testRoundTripCollectionCrud() {
    MongoDatabase original = getDatabaseWithPostfix();
    MongoDatabase replicated = getDatabaseWithPostfix();
    MongoCollection<Document> coll1 = original.getCollection("coll1");

    // Test existing messages will be round tripped
    insertMany(rangeClosed(1, 10), coll1);

    Properties sourceProperties = getSourceProperties(original);
    sourceProperties.put(FULL_DOCUMENT_CONFIG, "updateLookup");
    sourceProperties.put(COLLECTION_CONFIG, coll1.getNamespace().getCollectionName());
    addSourceConnector(sourceProperties);

    Properties sinkProperties = getSinkProperties(replicated, coll1);
    sinkProperties.put(COLLECTION_CONFIG, coll1.getNamespace().getCollectionName());
    addSinkConnector(sinkProperties);

    assertDatabase(original, replicated);

    // Test can handle multiple collections, with inserts and updates
    insertMany(rangeClosed(100, 110), coll1);
    coll1.updateMany(new Document(), Updates.set("test", 1));
    assertDatabase(original, replicated);

    // Test can handle replace and update operations
    insertMany(rangeClosed(120, 130), coll1);
    coll1.replaceOne(eq("_id", 1), new Document());
    coll1.replaceOne(eq("_id", 3), new Document());
    coll1.replaceOne(eq("_id", 5), new Document());

    assertDatabase(original, replicated);

    // Test handles delete operations
    insertMany(rangeClosed(140, 150), coll1);
    coll1.deleteMany(Filters.mod("_id", 2, 0));

    assertDatabase(original, replicated);
  }

  @Test
  @DisplayName("Ensure database CRUD operations can be round tripped")
  void testRoundTripDatabaseCrud() {
    assumeTrue(isGreaterThanThreeDotSix());
    MongoDatabase original = getDatabaseWithPostfix();
    MongoDatabase replicated = getDatabaseWithPostfix();
    MongoCollection<Document> coll1 = original.getCollection("coll1");
    MongoCollection<Document> coll2 = original.getCollection("coll2");

    // Test existing messages will be round tripped
    insertMany(rangeClosed(1, 10), coll1);

    Properties sourceProperties = getSourceProperties(original);
    sourceProperties.put(FULL_DOCUMENT_CONFIG, "updateLookup");
    addSourceConnector(sourceProperties);
    addSinkConnector(getSinkProperties(replicated, coll1, coll2));

    assertDatabase(original, replicated);

    // Test can handle multiple collections, with inserts and updates
    insertMany(rangeClosed(100, 110), coll1, coll2);
    coll1.updateMany(new Document(), Updates.set("test", 1));
    assertDatabase(original, replicated);

    // Test can handle replace and update operations
    insertMany(rangeClosed(120, 130), coll1, coll2);
    coll1.replaceOne(eq("_id", 1), new Document());
    coll1.replaceOne(eq("_id", 3), new Document());
    coll1.replaceOne(eq("_id", 5), new Document());
    coll2.updateMany(new Document(), Updates.set("newField", 1));

    assertDatabase(original, replicated);

    // Test handles delete operations
    insertMany(rangeClosed(140, 150), coll1, coll2);
    coll1.deleteMany(Filters.mod("_id", 2, 0));
    coll2.deleteMany(Filters.mod("_id", 3, 0));

    assertDatabase(original, replicated);
  }

  @Test
  @DisplayName("Ensure collection CRUD operations are replicated")
  void testPipelineBasedUpdatesCanBeRoundTripped() {
    // Port of scenarios from
    // mongo/jstests/change_streams/pipeline_style_updates_v2_oplog_entries.js
    assumeTrue(isGreaterThanFourDotZero() && !isGreaterThanFourDotFour());
    MongoDatabase original = getDatabaseWithPostfix();
    MongoDatabase replicated = getDatabaseWithPostfix();
    MongoCollection<Document> coll1 = original.getCollection("coll1");

    addSourceConnector(getSourceProperties(original));
    addSinkConnector(getSinkProperties(replicated, coll1));

    String giantString = Stream.generate(() -> "*").limit(1024).collect(Collectors.joining());
    String mediumString = Stream.generate(() -> "*").limit(128).collect(Collectors.joining());
    String smallString = Stream.generate(() -> "*").limit(32).collect(Collectors.joining());
    Bson filter = eq("_id", 100);

    // Testing pipeline-based update with $set
    coll1.insertOne(
        Document.parse(
            format(
                "{"
                    + "_id: 100,"
                    + "'a': 1,"
                    + "'b': 2,"
                    + "'arrayForSubdiff': ['%s', {a: '%s'}, 1, 2, 3],"
                    + "'arrayForReplacement': [0, 1, 2, 3],"
                    + "'giantStr': '%s'"
                    + "}",
                giantString, mediumString, giantString)));

    coll1.updateOne(
        filter,
        singletonList(
            Document.parse(
                format(
                    "{'$set': {"
                        + "'a': 2,"
                        + "'arrayForSubdiff': ['%s', {'a': '%s', 'b': 3}],"
                        + "'arrayForReplacement': [0],"
                        + "'c': 3"
                        + "}}",
                    giantString, mediumString))));

    assertDatabase(original, replicated);

    // Testing pipeline-based update with $unset
    coll1.insertOne(new Document());
    coll1.updateOne(filter, singletonList(Document.parse("{$unset: ['a']}")));
    assertDatabase(original, replicated);

    // Testing pipeline-based update with $replaceRoot
    coll1.insertOne(new Document());
    coll1.updateOne(
        filter,
        singletonList(
            Document.parse(
                format("{$replaceRoot: {newRoot: {_id: 100, 'giantStr': '%s'}}}", giantString))));
    assertDatabase(original, replicated);

    // Testing pipeline-based update with a complex pipeline
    coll1.insertOne(new Document());
    coll1.updateOne(
        filter,
        asList(
            Document.parse(
                format(
                    "{$replaceRoot: {"
                        + "    newRoot: {"
                        + "      _id: 100,"
                        + "          'giantStr': '%s',"
                        + "          'arr': [{'x': 1, 'y': '%s'}, '%s'],"
                        + "      'arr_a': [1, '%s'],"
                        + "      'arr_b': [[1, '%s'], '%s'],"
                        + "      'arr_c': [['%s', 1, 2, 3], '%s'],"
                        + "      'obj': {'x': {'a': 1, 'b': 1, 'c': ['%s', 1, 2, 3], 'str': '%s'}},"
                        + "    }"
                        + "  }"
                        + "}",
                    giantString,
                    smallString,
                    mediumString,
                    mediumString,
                    smallString,
                    mediumString,
                    smallString,
                    mediumString,
                    mediumString,
                    mediumString)),
            Document.parse("{$addFields: {'a': 'updated', 'b': 2, 'doc': {'a': {'0': 'foo'}}}}"),
            Document.parse(
                "{$project: {"
                    + "'a': true, 'giantStr': true, 'doc': true, 'arr': true, 'arr_a': true, "
                    + "'arr_b': true, 'arr_c': true, 'obj': true }}")));

    assertDatabase(original, replicated);

    // Testing pipeline-based update with modifications to nested elements
    coll1.insertOne(new Document());
    coll1.updateOne(
        filter,
        singletonList(
            Document.parse(
                format(
                    "{$replaceRoot: {"
                        + "    newRoot: {"
                        + "        _id: 100,"
                        + "        'giantStr': '%s',"
                        + "        'arr': [{'y': '%s'}, '%s'],"
                        + "        'arr_a': [2, '%s'],"
                        + "        'arr_b': [[2, '%s'], '%s'],"
                        + "        'arr_c': [['%s'], '%s'],"
                        + "        'obj': {'x': {'b': 2, 'c': ['%s'], 'str': '%s'}},"
                        + "    }"
                        + "}}",
                    giantString,
                    smallString,
                    mediumString,
                    mediumString,
                    smallString,
                    mediumString,
                    smallString,
                    mediumString,
                    mediumString,
                    mediumString))));
    assertDatabase(original, replicated);
  }

  @NotNull
  private Properties getSourceProperties(final MongoDatabase database) {
    Properties sourceProperties = new Properties();
    sourceProperties.put(DATABASE_CONFIG, database.getName());
    sourceProperties.put(TOPIC_PREFIX_CONFIG, "copy");
    sourceProperties.put(COPY_EXISTING_CONFIG, "true");
    return sourceProperties;
  }

  @NotNull
  private Properties getSinkProperties(
      final MongoDatabase destination, final MongoCollection<?>... sources) {
    Properties sinkProperties = createSinkProperties();

    String topics =
        Arrays.stream(sources)
            .map(coll -> format("copy.%s", coll.getNamespace().getFullName()))
            .collect(Collectors.joining(", "));
    sinkProperties.put("topics", topics);
    sinkProperties.put(DATABASE_CONFIG, destination.getName());
    sinkProperties.put(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, ChangeStreamHandler.class.getName());
    sinkProperties.put("key.converter", StringConverter.class.getName());
    sinkProperties.put("value.converter", StringConverter.class.getName());

    Arrays.stream(sources)
        .forEach(
            coll ->
                sinkProperties.put(
                    format(
                        TOPIC_OVERRIDE_CONFIG,
                        format("copy.%s", coll.getNamespace().getFullName()),
                        MongoSinkTopicConfig.COLLECTION_CONFIG),
                    coll.getNamespace().getCollectionName()));

    sinkProperties.remove(COLLECTION_CONFIG);
    return sinkProperties;
  }
}
