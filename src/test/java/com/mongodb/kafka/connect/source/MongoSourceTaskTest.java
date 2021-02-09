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

import static com.mongodb.kafka.connect.source.MongoSourceConfig.BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLATION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.FULL_DOCUMENT_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.SourceTestHelper.TEST_COLLECTION;
import static com.mongodb.kafka.connect.source.SourceTestHelper.TEST_DATABASE;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.changestream.FullDocument;

@ExtendWith(MockitoExtension.class)
@RunWith(JUnitPlatform.class)
class MongoSourceTaskTest {

  @Mock private MongoClient mongoClient;
  @Mock private MongoDatabase mongoDatabase;
  @Mock private MongoCollection<Document> mongoCollection;
  @Mock private ChangeStreamIterable<Document> changeStreamIterable;
  @Mock private MongoIterable<BsonDocument> mongoIterable;
  @Mock private MongoChangeStreamCursor<BsonDocument> mongoCursor;
  @Mock private SourceTaskContext context;
  @Mock private OffsetStorageReader offsetStorageReader;

  private static final BsonDocument RESUME_TOKEN = BsonDocument.parse("{resume: 'token'}");
  private static final Map<String, Object> OFFSET = singletonMap("_id", RESUME_TOKEN.toJson());

  @Test
  @DisplayName("test creates the expected collection cursor")
  void testCreatesExpectedCollectionCursor() {
    MongoSourceTask task = new MongoSourceTask();
    Map<String, String> cfgMap = new HashMap<>();
    cfgMap.put(CONNECTION_URI_CONFIG, "mongodb://localhost");
    cfgMap.put(DATABASE_CONFIG, TEST_DATABASE);
    cfgMap.put(COLLECTION_CONFIG, TEST_COLLECTION);
    MongoSourceConfig cfg = new MongoSourceConfig(cfgMap);

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION)).thenReturn(mongoCollection);
    when(mongoCollection.watch()).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).getDatabase(TEST_DATABASE);
    verify(mongoDatabase, times(1)).getCollection(TEST_COLLECTION);
    verify(mongoCollection, times(1)).watch();
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();

    // Pipeline
    resetMocks();
    cfgMap.put(PIPELINE_CONFIG, "[{$match: {operationType: 'insert'}}]");
    cfg = new MongoSourceConfig(cfgMap);

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION)).thenReturn(mongoCollection);
    when(mongoCollection.watch(cfg.getPipeline().get())).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).getDatabase(TEST_DATABASE);
    verify(mongoDatabase, times(1)).getCollection(TEST_COLLECTION);
    verify(mongoCollection, times(1)).watch(cfg.getPipeline().get());
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();

    // Complex
    resetMocks();
    cfgMap.put(BATCH_SIZE_CONFIG, "101");

    FullDocument fullDocument = FullDocument.UPDATE_LOOKUP;
    cfgMap.put(FULL_DOCUMENT_CONFIG, fullDocument.getValue());
    Collation collation =
        Collation.builder()
            .locale("en")
            .caseLevel(true)
            .collationCaseFirst(CollationCaseFirst.OFF)
            .collationStrength(CollationStrength.IDENTICAL)
            .collationAlternate(CollationAlternate.SHIFTED)
            .collationMaxVariable(CollationMaxVariable.SPACE)
            .numericOrdering(true)
            .normalization(true)
            .backwards(true)
            .build();
    cfgMap.put(COLLATION_CONFIG, collation.asDocument().toJson());

    cfg = new MongoSourceConfig(cfgMap);

    task.initialize(context);
    when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
    when(offsetStorageReader.offset(task.createPartitionMap(cfg))).thenReturn(OFFSET);

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.getCollection(TEST_COLLECTION)).thenReturn(mongoCollection);
    when(mongoCollection.watch(cfg.getPipeline().get())).thenReturn(changeStreamIterable);
    when(changeStreamIterable.batchSize(101)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.fullDocument(fullDocument)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.collation(collation)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.startAfter(RESUME_TOKEN)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).getDatabase(TEST_DATABASE);
    verify(mongoDatabase, times(1)).getCollection(TEST_COLLECTION);
    verify(mongoCollection, times(1)).watch(cfg.getPipeline().get());
    verify(changeStreamIterable, times(1)).batchSize(101);
    verify(changeStreamIterable, times(1)).fullDocument(fullDocument);
    verify(changeStreamIterable, times(1)).collation(collation);
    verify(changeStreamIterable, times(1)).startAfter(RESUME_TOKEN);
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();
  }

  @Test
  @DisplayName("test creates the expected database cursor")
  void testCreatesExpectedDatabaseCursor() {
    MongoSourceTask task = new MongoSourceTask();
    Map<String, String> cfgMap = new HashMap<>();
    cfgMap.put(CONNECTION_URI_CONFIG, "mongodb://localhost");
    cfgMap.put(DATABASE_CONFIG, TEST_DATABASE);
    MongoSourceConfig cfg = new MongoSourceConfig(cfgMap);

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.watch()).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).getDatabase(TEST_DATABASE);
    verify(mongoDatabase, times(1)).watch();
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();

    // Pipeline
    resetMocks();
    cfgMap.put(PIPELINE_CONFIG, "[{$match: {operationType: 'insert'}}]");
    cfg = new MongoSourceConfig(cfgMap);

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.watch(cfg.getPipeline().get())).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).getDatabase(TEST_DATABASE);
    verify(mongoDatabase, times(1)).watch(cfg.getPipeline().get());
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();

    // Complex
    resetMocks();
    cfgMap.put(BATCH_SIZE_CONFIG, "101");

    FullDocument fullDocument = FullDocument.UPDATE_LOOKUP;
    cfgMap.put(FULL_DOCUMENT_CONFIG, fullDocument.getValue());
    Collation collation =
        Collation.builder()
            .locale("en")
            .caseLevel(true)
            .collationCaseFirst(CollationCaseFirst.OFF)
            .collationStrength(CollationStrength.IDENTICAL)
            .collationAlternate(CollationAlternate.SHIFTED)
            .collationMaxVariable(CollationMaxVariable.SPACE)
            .numericOrdering(true)
            .normalization(true)
            .backwards(true)
            .build();
    cfgMap.put(COLLATION_CONFIG, collation.asDocument().toJson());

    cfg = new MongoSourceConfig(cfgMap);

    task.initialize(context);
    when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
    when(offsetStorageReader.offset(task.createPartitionMap(cfg))).thenReturn(OFFSET);

    when(mongoClient.getDatabase(TEST_DATABASE)).thenReturn(mongoDatabase);
    when(mongoDatabase.watch(cfg.getPipeline().get())).thenReturn(changeStreamIterable);
    when(changeStreamIterable.batchSize(101)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.fullDocument(fullDocument)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.collation(collation)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.startAfter(RESUME_TOKEN)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).getDatabase(TEST_DATABASE);
    verify(mongoDatabase, times(1)).watch(cfg.getPipeline().get());
    verify(changeStreamIterable, times(1)).batchSize(101);
    verify(changeStreamIterable, times(1)).fullDocument(fullDocument);
    verify(changeStreamIterable, times(1)).collation(collation);
    verify(changeStreamIterable, times(1)).startAfter(RESUME_TOKEN);
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();
  }

  @Test
  @DisplayName("test creates the expected client cursor")
  void testCreatesExpectedClientCursor() {
    MongoSourceTask task = new MongoSourceTask();
    Map<String, String> cfgMap = new HashMap<>();
    cfgMap.put(CONNECTION_URI_CONFIG, "mongodb://localhost");
    MongoSourceConfig cfg = new MongoSourceConfig(cfgMap);

    when(mongoClient.watch()).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).watch();
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();

    // Pipeline
    resetMocks();
    cfgMap.put(PIPELINE_CONFIG, "[{$match: {operationType: 'insert'}}]");
    cfg = new MongoSourceConfig(cfgMap);

    when(mongoClient.watch(cfg.getPipeline().get())).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).watch(cfg.getPipeline().get());
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();

    // Complex
    resetMocks();
    cfgMap.put(BATCH_SIZE_CONFIG, "101");

    FullDocument fullDocument = FullDocument.UPDATE_LOOKUP;
    cfgMap.put(FULL_DOCUMENT_CONFIG, fullDocument.getValue());
    Collation collation =
        Collation.builder()
            .locale("en")
            .caseLevel(true)
            .collationCaseFirst(CollationCaseFirst.OFF)
            .collationStrength(CollationStrength.IDENTICAL)
            .collationAlternate(CollationAlternate.SHIFTED)
            .collationMaxVariable(CollationMaxVariable.SPACE)
            .numericOrdering(true)
            .normalization(true)
            .backwards(true)
            .build();
    cfgMap.put(COLLATION_CONFIG, collation.asDocument().toJson());
    cfg = new MongoSourceConfig(cfgMap);

    task.initialize(context);
    when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
    when(offsetStorageReader.offset(task.createPartitionMap(cfg))).thenReturn(OFFSET);

    when(mongoClient.watch(cfg.getPipeline().get())).thenReturn(changeStreamIterable);
    when(changeStreamIterable.batchSize(101)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.fullDocument(fullDocument)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.collation(collation)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.startAfter(RESUME_TOKEN)).thenReturn(changeStreamIterable);
    when(changeStreamIterable.withDocumentClass(BsonDocument.class)).thenReturn(mongoIterable);
    when(mongoIterable.cursor()).thenReturn(mongoCursor);

    task.createCursor(cfg, mongoClient);

    verify(mongoClient, times(1)).watch(cfg.getPipeline().get());
    verify(changeStreamIterable, times(1)).batchSize(101);
    verify(changeStreamIterable, times(1)).fullDocument(fullDocument);
    verify(changeStreamIterable, times(1)).collation(collation);
    verify(changeStreamIterable, times(1)).startAfter(RESUME_TOKEN);
    verify(changeStreamIterable, times(1)).withDocumentClass(BsonDocument.class);
    verify(mongoIterable, times(1)).cursor();
  }

  @Test
  @DisplayName("test handles legacy offsets")
  void testHandlesLegacyOffsets() {
    MongoSourceTask task = new MongoSourceTask();
    Map<String, String> cfgMap = new HashMap<>();
    cfgMap.put(CONNECTION_URI_CONFIG, "mongodb://localhost");
    cfgMap.put(DATABASE_CONFIG, TEST_DATABASE);
    cfgMap.put(COLLECTION_CONFIG, TEST_COLLECTION);
    MongoSourceConfig cfg = new MongoSourceConfig(cfgMap);

    task.initialize(context);
    when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
    when(offsetStorageReader.offset(task.createPartitionMap(cfg))).thenReturn(null);
    when(offsetStorageReader.offset(task.createLegacyPartitionMap(cfg))).thenReturn(OFFSET);

    assertEquals(OFFSET, task.getOffset(cfg));
  }

  @Test
  @DisplayName("test creates the expected partition map")
  void testCreatesTheExpectedPartitionMap() {
    MongoSourceTask task = new MongoSourceTask();
    Map<String, String> cfgMap = new HashMap<>();
    cfgMap.put(CONNECTION_URI_CONFIG, "mongodb+srv://user:password@localhost/");
    cfgMap.put(DATABASE_CONFIG, TEST_DATABASE);
    cfgMap.put(COLLECTION_CONFIG, TEST_COLLECTION);
    MongoSourceConfig cfg = new MongoSourceConfig(cfgMap);

    assertEquals(
        format("mongodb+srv://localhost/%s.%s", TEST_DATABASE, TEST_COLLECTION),
        task.createDefaultPartitionName(cfg));
    assertEquals(
        format("mongodb+srv://user:password@localhost//%s.%s", TEST_DATABASE, TEST_COLLECTION),
        task.createLegacyPartitionName(cfg));

    cfgMap.put(CONNECTION_URI_CONFIG, "mongodb://localhost/");
    cfg = new MongoSourceConfig(cfgMap);
    assertEquals(
        format("mongodb://localhost/%s.%s", TEST_DATABASE, TEST_COLLECTION),
        task.createDefaultPartitionName(cfg));
    assertEquals(
        format("mongodb://localhost//%s.%s", TEST_DATABASE, TEST_COLLECTION),
        task.createLegacyPartitionName(cfg));

    cfgMap.remove(COLLECTION_CONFIG);
    cfg = new MongoSourceConfig(cfgMap);
    assertEquals(
        format("mongodb://localhost/%s", TEST_DATABASE), task.createDefaultPartitionName(cfg));
    assertEquals(
        format("mongodb://localhost//%s.", TEST_DATABASE), task.createLegacyPartitionName(cfg));

    cfgMap.remove(DATABASE_CONFIG);
    cfg = new MongoSourceConfig(cfgMap);
    assertEquals("mongodb://localhost/", task.createDefaultPartitionName(cfg));
    assertEquals("mongodb://localhost//.", task.createLegacyPartitionName(cfg));
  }

  private void resetMocks() {
    reset(
        mongoClient,
        mongoDatabase,
        mongoCollection,
        changeStreamIterable,
        mongoIterable,
        mongoCursor,
        context,
        offsetStorageReader);
  }
}
