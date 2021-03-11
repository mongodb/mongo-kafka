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

import static com.mongodb.kafka.connect.source.MongoCopyDataManager.NAMESPACE_FIELD;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_NAMESPACE_REGEX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_QUEUE_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.SourceTestHelper.TEST_COLLECTION;
import static com.mongodb.kafka.connect.source.SourceTestHelper.TEST_DATABASE;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue.documentToByteArray;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
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
import org.bson.codecs.BsonDocumentCodec;
import org.bson.conversions.Bson;

import com.mongodb.Function;
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

  private static final String CHANGE_STREAM_DOCUMENT_TEMPLATE =
      "{'_id': {'_id': %s, 'copy': true}, "
          + "'operationType': 'insert', '%%s': {'db': '%s', 'coll': '%s'}, "
          + "'documentKey': {'_id': %s}, "
          + "'fullDocument': {'_id': %s, 'a': 'a', 'b': %s}}";

  @Mock private MongoClient mongoClient;
  @Mock private MongoDatabase mongoDatabase;
  @Mock private MongoDatabase mongoDatabaseAlt;
  @Mock private MongoCollection<RawBsonDocument> mongoCollection;
  @Mock private MongoCollection<RawBsonDocument> mongoCollectionAlt;
  @Mock private AggregateIterable<RawBsonDocument> aggregateIterable;
  @Mock private AggregateIterable<RawBsonDocument> aggregateIterableAlt;
  @Mock private MongoCursor<RawBsonDocument> cursor;
  @Mock private MongoCursor<RawBsonDocument> cursorAlt;

  @Test
  @DisplayName("test returns the expected collection results")
  void testReturnsTheExpectedCollectionResults() {
    String jsonTemplate = createTemplate(1);

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION, RawBsonDocument.class))
        .thenReturn(mongoCollection);
    when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);
    when(cursor.hasNext()).thenReturn(true, false);
    when(cursor.next()).thenReturn(createInput(jsonTemplate));

    List<Optional<BsonDocument>> results;
    try (MongoCopyDataManager copyExistingDataManager =
        new MongoCopyDataManager(createSourceConfig(), mongoClient)) {
      sleep();
      results = asList(copyExistingDataManager.poll(), copyExistingDataManager.poll());
    }

    List<Optional<BsonDocument>> expected = asList(createOutput(jsonTemplate), Optional.empty());
    assertEquals(expected, results);
  }

  @Test
  @DisplayName("test applies the expected pipelines")
  void testAppliesTheExpectedPipelines() {
    String jsonTemplate = createTemplate(1);
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

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION, RawBsonDocument.class))
        .thenReturn(mongoCollection);
    when(mongoCollection.aggregate(expectedPipeline)).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);
    when(cursor.hasNext()).thenReturn(true, false);
    when(cursor.next()).thenReturn(createInput(jsonTemplate));

    List<Optional<BsonDocument>> results;
    try (MongoCopyDataManager copyExistingDataManager =
        new MongoCopyDataManager(sourceConfig, mongoClient)) {
      sleep();
      results = asList(copyExistingDataManager.poll(), copyExistingDataManager.poll());
    }

    List<Optional<BsonDocument>> expected = asList(createOutput(jsonTemplate), Optional.empty());
    assertEquals(expected, results);
  }

  @Test
  @DisplayName("test blocks adding docs to the queue")
  void testBlocksAddingResultsToTheQueue() {
    List<String> templates =
        IntStream.range(0, 10)
            .mapToObj(MongoCopyDataManagerTest::createTemplate)
            .collect(Collectors.toList());

    List<RawBsonDocument> inputDocs =
        templates.stream().map(MongoCopyDataManagerTest::createInput).collect(Collectors.toList());

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION, RawBsonDocument.class))
        .thenReturn(mongoCollection);
    when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);

    Boolean[] hasNextResponses = new Boolean[inputDocs.size()];
    Arrays.fill(hasNextResponses, true);
    hasNextResponses[hasNextResponses.length - 1] = false;

    when(cursor.hasNext()).thenReturn(true, hasNextResponses);
    when(cursor.next())
        .thenReturn(
            inputDocs.get(0),
            inputDocs
                .subList(1, inputDocs.size())
                .toArray(new RawBsonDocument[inputDocs.size() - 1]));

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

    List<Optional<BsonDocument>> expected =
        templates.stream().map(MongoCopyDataManagerTest::createOutput).collect(Collectors.toList());
    expected.add(Optional.empty());
    assertEquals(expected, results);
  }

  @Test
  @DisplayName("test returns the expected database results")
  void testReturnsTheExpectedDatabaseResults() {
    String template1 = createTemplate(1, "myDB", "coll1");
    String template2 = createTemplate(2, "myDB", "coll2");

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.listCollectionNames()).thenReturn(new MockMongoIterable<>("coll1", "coll2"));
    when(mongoDatabase.getCollection("coll1", RawBsonDocument.class)).thenReturn(mongoCollection);
    when(mongoDatabase.getCollection("coll2", RawBsonDocument.class))
        .thenReturn(mongoCollectionAlt);

    when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);
    when(cursor.hasNext()).thenReturn(true, false);
    when(cursor.next()).thenReturn(createInput(template1));

    when(mongoCollectionAlt.aggregate(anyList())).thenReturn(aggregateIterableAlt);
    doCallRealMethod().when(aggregateIterableAlt).forEach(any(Consumer.class));
    when(aggregateIterableAlt.iterator()).thenReturn(cursorAlt);
    when(cursorAlt.hasNext()).thenReturn(true, false);
    when(cursorAlt.next()).thenReturn(createInput(template2));

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
    List<Optional<BsonDocument>> expected =
        asList(createOutput(template1), createOutput(template2), Optional.empty());

    assertTrue(results.containsAll(expected));
    assertEquals(results.get(results.size() - 1), Optional.empty());
  }

  @Test
  @DisplayName("test returns the expected client results")
  void testReturnsTheExpectedClientResults() {
    String template1 = createTemplate(1, "db1", "coll1");
    String template2 = createTemplate(2, "db1", "coll1");
    String template3 = createTemplate(1, "db2", "coll2");

    when(mongoClient.listDatabaseNames()).thenReturn(new MockMongoIterable<>("db1", "db2"));
    when(mongoClient.getDatabase("db1")).thenReturn(mongoDatabase);
    when(mongoClient.getDatabase("db2")).thenReturn(mongoDatabaseAlt);
    when(mongoDatabase.listCollectionNames()).thenReturn(new MockMongoIterable<>("coll1"));
    when(mongoDatabaseAlt.listCollectionNames()).thenReturn(new MockMongoIterable<>("coll2"));

    when(mongoDatabase.getCollection("coll1", RawBsonDocument.class)).thenReturn(mongoCollection);
    when(mongoDatabaseAlt.getCollection("coll2", RawBsonDocument.class))
        .thenReturn(mongoCollectionAlt);

    when(mongoCollection.aggregate(anyList())).thenReturn(aggregateIterable);
    doCallRealMethod().when(aggregateIterable).forEach(any(Consumer.class));
    when(aggregateIterable.iterator()).thenReturn(cursor);
    when(cursor.hasNext()).thenReturn(true, true, false);
    when(cursor.next()).thenReturn(createInput(template1), createInput(template2));

    when(mongoCollectionAlt.aggregate(anyList())).thenReturn(aggregateIterableAlt);
    doCallRealMethod().when(aggregateIterableAlt).forEach(any(Consumer.class));
    when(aggregateIterableAlt.iterator()).thenReturn(cursorAlt);
    when(cursorAlt.hasNext()).thenReturn(true, false);
    when(cursorAlt.next()).thenReturn(createInput(template3));

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
            createOutput(template1),
            createOutput(template2),
            createOutput(template3),
            Optional.empty());

    assertTrue(results.containsAll(expected));
    assertEquals(results.get(results.size() - 1), Optional.empty());
  }

  @Test
  @DisplayName("test selects the expected namespaces")
  void testSelectsTheExpectedNamespaces() {
    when(mongoClient.listDatabaseNames()).thenReturn(new MockMongoIterable<>("db1", "db2"));

    when(mongoClient.getDatabase("db1")).thenReturn(mongoDatabase);
    when(mongoClient.getDatabase("db2")).thenReturn(mongoDatabaseAlt);

    when(mongoDatabase.listCollectionNames())
        .thenReturn(new MockMongoIterable<>("coll1", "coll2", "coll3"));
    when(mongoDatabaseAlt.listCollectionNames())
        .thenReturn(new MockMongoIterable<>("coll1", "coll2"));

    assertAll(
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put(DATABASE_CONFIG, "db1");
          map.put(COLLECTION_CONFIG, "coll1");
          MongoSourceConfig config = new MongoSourceConfig(map);

          assertEquals(
              singletonList(new MongoNamespace("db1", "coll1")),
              MongoCopyDataManager.selectNamespaces(config, mongoClient));
        },
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put(DATABASE_CONFIG, "db1");
          map.put(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "coll(1|2)$");
          MongoSourceConfig config = new MongoSourceConfig(map);

          assertEquals(
              asList(new MongoNamespace("db1", "coll1"), new MongoNamespace("db1", "coll2")),
              MongoCopyDataManager.selectNamespaces(config, mongoClient));
        },
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "^db(1|2)\\.coll(1|2)$");
          MongoSourceConfig config = new MongoSourceConfig(map);
          assertEquals(
              asList(
                  new MongoNamespace("db1", "coll1"),
                  new MongoNamespace("db1", "coll2"),
                  new MongoNamespace("db2", "coll1"),
                  new MongoNamespace("db2", "coll2")),
              MongoCopyDataManager.selectNamespaces(config, mongoClient));
        },
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "^db(1|2)");
          MongoSourceConfig config = new MongoSourceConfig(map);

          assertEquals(
              asList(
                  new MongoNamespace("db1", "coll1"),
                  new MongoNamespace("db1", "coll2"),
                  new MongoNamespace("db1", "coll3"),
                  new MongoNamespace("db2", "coll1"),
                  new MongoNamespace("db2", "coll2")),
              MongoCopyDataManager.selectNamespaces(config, mongoClient));
        },
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "^db2");
          MongoSourceConfig config = new MongoSourceConfig(map);

          assertEquals(
              Arrays.asList(new MongoNamespace("db2", "coll1"), new MongoNamespace("db2", "coll2")),
              MongoCopyDataManager.selectNamespaces(config, mongoClient));
        },
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put(DATABASE_CONFIG, "db1");
          map.put(COLLECTION_CONFIG, "coll1");
          map.put(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "^db1\\.coll2$");
          MongoSourceConfig config = new MongoSourceConfig(map);

          assertEquals(emptyList(), MongoCopyDataManager.selectNamespaces(config, mongoClient));
        },
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "coll2$");
          MongoSourceConfig config = new MongoSourceConfig(map);

          assertEquals(
              Arrays.asList(new MongoNamespace("db1", "coll2"), new MongoNamespace("db2", "coll2")),
              MongoCopyDataManager.selectNamespaces(config, mongoClient));
        },
        () -> {
          MongoSourceConfig config = new MongoSourceConfig(new HashMap<>());

          assertEquals(
              asList(
                  new MongoNamespace("db1", "coll1"),
                  new MongoNamespace("db1", "coll2"),
                  new MongoNamespace("db1", "coll3"),
                  new MongoNamespace("db2", "coll1"),
                  new MongoNamespace("db2", "coll2")),
              MongoCopyDataManager.selectNamespaces(config, mongoClient));
        });
  }

  @Test
  @DisplayName("test convert document")
  void testConvertDocument() {
    String jsonTemplate = createTemplate(1);
    RawBsonDocument inputDocument = createInput(jsonTemplate);
    RawBsonDocument expectedDocument =
        new RawBsonDocument(
            BsonDocument.parse(format(jsonTemplate, "ns")), new BsonDocumentCodec());
    RawBsonDocument converted = MongoCopyDataManager.convertDocument(inputDocument);

    assertEquals(expectedDocument, converted);
    assertEquals(expectedDocument, new RawBsonDocument(documentToByteArray(converted)));
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

  private static String createTemplate(final int id) {
    return createTemplate(id, "myDB", "myColl");
  }

  private static String createTemplate(final int id, final String dbName, final String collName) {
    return format(CHANGE_STREAM_DOCUMENT_TEMPLATE, id, dbName, collName, id, id, id);
  }

  private static RawBsonDocument createInput(final String json) {
    return RawBsonDocument.parse(format(json, NAMESPACE_FIELD));
  }

  private static Optional<BsonDocument> createOutput(final String json) {
    return Optional.of(RawBsonDocument.parse(format(json, "ns")));
  }

  private static final class MockMongoIterable<T> implements MongoIterable<T> {

    private final Collection<T> result;

    private MockMongoIterable(final T... result) {
      this.result = asList(result);
    }

    @Override
    public MongoCursor<T> iterator() {
      throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public MongoCursor<T> cursor() {
      throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public T first() {
      return null;
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
      throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
      target.addAll(result);
      return target;
    }

    @Override
    public MongoIterable<T> batchSize(final int batchSize) {
      throw new UnsupportedOperationException("Unsupported operation");
    }
  }
}
