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
package com.mongodb.kafka.connect.source;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_QUEUE_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.SourceTestHelper.TEST_COLLECTION;
import static com.mongodb.kafka.connect.source.SourceTestHelper.TEST_DATABASE;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.MongoNamespace;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

@ExtendWith(MockitoExtension.class)
@RunWith(JUnitPlatform.class)
@SuppressWarnings("unchecked")
class MongoCopyDataManagerTest {

  @Mock private MongoClient mongoClient;
  @Mock private MongoDatabase mongoDatabase;
  @Mock private MongoDatabase mongoDatabaseAlt;
  @Mock private MongoCollection<RawBsonDocument> mongoCollection;
  @Mock private MongoCollection<RawBsonDocument> mongoCollectionAlt;
  @Mock private AggregateIterable<RawBsonDocument> aggregateIterable;
  @Mock private AggregateIterable<RawBsonDocument> aggregateIterableAlt;
  @Mock private MongoCursor<RawBsonDocument> cursor;
  @Mock private MongoCursor<RawBsonDocument> cursorAlt;
  @Mock private MongoIterable<String> databaseNamesIterable;
  @Mock private MongoIterable<String> collectionNamesIterable;
  @Mock private MongoIterable<String> collectionNamesIterableAlt;

  @Test
  @DisplayName("test returns the expected collection results")
  void testReturnsTheExpectedCollectionResults() {
    RawBsonDocument result =
        RawBsonDocument.parse(
            "{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'myDB', 'coll': 'myColl'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'a': 'a', 'b': 121}}");

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION, RawBsonDocument.class))
        .thenReturn(mongoCollection);
    when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);
    when(cursor.hasNext()).thenReturn(true, false);
    when(cursor.next()).thenReturn(result);

    List<Optional<BsonDocument>> results;
    try (MongoCopyDataManager copyExistingDataManager =
        new MongoCopyDataManager(createSourceConfig(), mongoClient)) {
      sleep();
      results = asList(copyExistingDataManager.poll(), copyExistingDataManager.poll());
    }

    List<Optional<BsonDocument>> expected = asList(Optional.of(result), Optional.empty());
    assertEquals(expected, results);
  }

  @Test
  @DisplayName("test applies the expected pipelines")
  void testAppliesTheExpectedPipelines() {
    String copyPipeline = "[{'$match': {'closed': false}}]";
    String pipeline = "[{'$match': {'status': 'A'}}]";

    MongoSourceConfig sourceConfig =
        createSourceConfig(
            format(
                "{'%s': \"%s\", '%s': \"%s\"}",
                COPY_EXISTING_PIPELINE_CONFIG, copyPipeline, PIPELINE_CONFIG, pipeline));

    List<Bson> expectedPipeline =
        MongoCopyDataManager.createPipeline(
            sourceConfig, new MongoNamespace(TEST_DATABASE, TEST_COLLECTION));

    RawBsonDocument result =
        RawBsonDocument.parse(
            "{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'myDB', 'coll': 'myColl'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'a': 'a', 'b': 121}}");

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION, RawBsonDocument.class))
        .thenReturn(mongoCollection);
    when(mongoCollection.aggregate(expectedPipeline)).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);
    when(cursor.hasNext()).thenReturn(true, false);
    when(cursor.next()).thenReturn(result);

    List<Optional<BsonDocument>> results;
    try (MongoCopyDataManager copyExistingDataManager =
        new MongoCopyDataManager(sourceConfig, mongoClient)) {
      sleep();
      results = asList(copyExistingDataManager.poll(), copyExistingDataManager.poll());
    }

    List<Optional<BsonDocument>> expected = asList(Optional.of(result), Optional.empty());
    assertEquals(expected, results);
  }

  @Test
  @DisplayName("test blocks adding docs to the queue")
  void testBlocksAddingResultsToTheQueue() {
    List<RawBsonDocument> docs =
        IntStream.range(0, 10)
            .mapToObj(i -> RawBsonDocument.parse(format("{'_id': {'_id': %s, 'copy': true}}", i)))
            .collect(Collectors.toList());

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION, RawBsonDocument.class))
        .thenReturn(mongoCollection);
    when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);

    Boolean[] hasNextResponses = new Boolean[docs.size()];
    Arrays.fill(hasNextResponses, true);
    hasNextResponses[hasNextResponses.length - 1] = false;

    when(cursor.hasNext()).thenReturn(true, hasNextResponses);
    when(cursor.next())
        .thenReturn(
            docs.get(0),
            docs.subList(1, docs.size()).toArray(new RawBsonDocument[docs.size() - 1]));

    List<Optional<BsonDocument>> results;
    try (MongoCopyDataManager copyExistingDataManager =
        new MongoCopyDataManager(
            createSourceConfig(COPY_EXISTING_QUEUE_SIZE_CONFIG, "1"), mongoClient)) {
      sleep();
      results =
          IntStream.range(0, 11)
              .mapToObj(
                  i -> {
                    sleep(200);
                    return copyExistingDataManager.poll();
                  })
              .collect(Collectors.toList());
    }

    List<Optional<RawBsonDocument>> expected =
        docs.stream().map(Optional::of).collect(Collectors.toList());
    expected.add(Optional.empty());
    assertEquals(expected, results);
  }

  @Test
  @DisplayName("test returns the expected database results")
  void testReturnsTheExpectedDatabaseResults() {
    RawBsonDocument myDbColl1Result =
        RawBsonDocument.parse(
            "{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'myDB', 'coll': 'coll1'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'a': 'a', 'b': 121}}");

    RawBsonDocument myDbColl2Result =
        RawBsonDocument.parse(
            "{'_id': {'_id': 2, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'myDB', 'coll': 'coll2'}, "
                + "'documentKey': {'_id': 2}, "
                + "'fullDocument': {'_id': 2, 'a': 'b', 'b': 212}}");

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.listCollectionNames()).thenReturn(collectionNamesIterable);
    doAnswer(
            i -> {
              List<String> list = (List<String>) i.getArgument(0, ArrayList.class);
              list.add("coll1");
              list.add("coll2");
              return list;
            })
        .when(collectionNamesIterable)
        .into(any(ArrayList.class));
    when(mongoDatabase.getCollection("coll1", RawBsonDocument.class)).thenReturn(mongoCollection);
    when(mongoDatabase.getCollection("coll2", RawBsonDocument.class))
        .thenReturn(mongoCollectionAlt);

    when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);
    when(cursor.hasNext()).thenReturn(true, false);
    when(cursor.next()).thenReturn(myDbColl1Result);

    when(mongoCollectionAlt.aggregate(anyList())).thenReturn(aggregateIterableAlt);
    doCallRealMethod().when(aggregateIterableAlt).forEach(any(Consumer.class));
    when(aggregateIterableAlt.iterator()).thenReturn(cursorAlt);
    when(cursorAlt.hasNext()).thenReturn(true, false);
    when(cursorAlt.next()).thenReturn(myDbColl2Result);

    Map<String, String> dbConfig = createConfigMap();
    dbConfig.remove(COLLECTION_CONFIG);

    List<Optional<BsonDocument>> results;
    try (MongoCopyDataManager copyExistingDataManager =
        new MongoCopyDataManager(new MongoSourceConfig(dbConfig), mongoClient)) {
      sleep();
      results =
          asList(
              copyExistingDataManager.poll(),
              copyExistingDataManager.poll(),
              copyExistingDataManager.poll());
    }
    List<Optional<RawBsonDocument>> expected =
        asList(Optional.of(myDbColl1Result), Optional.of(myDbColl2Result), Optional.empty());

    assertTrue(results.containsAll(expected));
    assertEquals(results.get(results.size() - 1), Optional.empty());
  }

  @Test
  @DisplayName("test returns the expected client results")
  void testReturnsTheExpectedClientResults() {
    RawBsonDocument db1Coll1Result1 =
        RawBsonDocument.parse(
            "{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'db1', 'coll': 'coll1'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'a': 'a', 'b': 121}}");
    RawBsonDocument db1Coll1Result2 =
        RawBsonDocument.parse(
            "{'_id': {'_id': 2, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'db1', 'coll': 'coll1'}, "
                + "'documentKey': {'_id': 2}, "
                + "'fullDocument': {'_id': 2, 'a': 'aa', 'b': 111}}");
    RawBsonDocument db2Coll2Result1 =
        RawBsonDocument.parse(
            "{'_id': {'_id': 1, 'copy': true}, "
                + "'operationType': 'insert', 'ns': {'db': 'db2', 'coll': 'coll2'}, "
                + "'documentKey': {'_id': 1}, "
                + "'fullDocument': {'_id': 1, 'c': 'c', 'd': 999}}");

    when(mongoClient.listDatabaseNames()).thenReturn(databaseNamesIterable);
    doAnswer(
            i -> {
              List<String> list = (List<String>) i.getArgument(0, ArrayList.class);
              list.add("db1");
              list.add("db2");
              return list;
            })
        .when(databaseNamesIterable)
        .into(any(ArrayList.class));

    when(mongoClient.getDatabase("db1")).thenReturn(mongoDatabase);
    when(mongoClient.getDatabase("db2")).thenReturn(mongoDatabaseAlt);
    when(mongoDatabase.listCollectionNames()).thenReturn(collectionNamesIterable);
    when(mongoDatabaseAlt.listCollectionNames()).thenReturn(collectionNamesIterableAlt);

    doAnswer(
            i -> {
              List<String> list = (List<String>) i.getArgument(0, ArrayList.class);
              list.add("coll1");
              return list;
            })
        .when(collectionNamesIterable)
        .into(any(ArrayList.class));

    doAnswer(
            i -> {
              List<String> list = (List<String>) i.getArgument(0, ArrayList.class);
              list.add("coll2");
              return list;
            })
        .when(collectionNamesIterableAlt)
        .into(any(ArrayList.class));

    when(mongoDatabase.getCollection("coll1", RawBsonDocument.class)).thenReturn(mongoCollection);
    when(mongoDatabaseAlt.getCollection("coll2", RawBsonDocument.class))
        .thenReturn(mongoCollectionAlt);

    when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);
    when(cursor.hasNext()).thenReturn(true, true, false);
    when(cursor.next()).thenReturn(db1Coll1Result1, db1Coll1Result2);

    when(mongoCollectionAlt.aggregate(anyList())).thenReturn(aggregateIterableAlt);
    doCallRealMethod().when(aggregateIterableAlt).forEach(any(Consumer.class));
    when(aggregateIterableAlt.iterator()).thenReturn(cursorAlt);
    when(cursorAlt.hasNext()).thenReturn(true, false);
    when(cursorAlt.next()).thenReturn(db2Coll2Result1);

    List<Optional<BsonDocument>> results;
    try (MongoCopyDataManager copyExistingDataManager =
        new MongoCopyDataManager(new MongoSourceConfig(new HashMap<>()), mongoClient)) {
      sleep();
      results =
          asList(
              copyExistingDataManager.poll(),
              copyExistingDataManager.poll(),
              copyExistingDataManager.poll(),
              copyExistingDataManager.poll());
    }
    List<Optional<BsonDocument>> expected =
        asList(
            Optional.of(db1Coll1Result1),
            Optional.of(db1Coll1Result2),
            Optional.of(db2Coll2Result1),
            Optional.empty());

    assertTrue(results.containsAll(expected));
    assertEquals(results.get(results.size() - 1), Optional.empty());
  }

  private void sleep(final int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  private void sleep() {
    sleep(500);
  }
}
