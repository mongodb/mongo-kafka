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
import static com.mongodb.kafka.connect.source.MongoSourceConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_TOLERANCE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OFFSET_PARTITION_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceTask.ID_FIELD;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.mongodb.kafka.connect.source.statistics.JmxStatisticsManager;

@SuppressWarnings("unchecked")
class SourceCheckpointTest {

  @Test
  void replicaSetNameIsStableAcrossHostChanges() {
    MongoSourceConfig original =
        config("mongodb://b.example:27017,a.example:27017/?replicaSet=rs0");
    MongoSourceConfig edited = config("mongodb://a.example:27017,c.example:27017/?replicaSet=rs0");

    assertEquals("mongodb://rs0/db.coll", MongoSourceTask.createDefaultPartitionName(original));
    assertEquals(
        MongoSourceTask.createDefaultPartitionName(original),
        MongoSourceTask.createDefaultPartitionName(edited));
    assertEquals(
        "mongodb://a.example:27017,b.example:27017/db.coll",
        MongoSourceTask.createLegacyPartitionName(original));
    assertNotEquals(
        MongoSourceTask.createLegacyPartitionName(original),
        MongoSourceTask.createLegacyPartitionName(edited));
  }

  @Test
  void hostOrderDoesNotChangeTheLegacyPartition() {
    MongoSourceConfig forward = config("mongodb://a.example:27017,b.example:27017");
    MongoSourceConfig reversed = config("mongodb://b.example:27017,a.example:27017");

    assertEquals(
        MongoSourceTask.createDefaultPartitionName(forward),
        MongoSourceTask.createDefaultPartitionName(reversed));
    assertEquals(
        "mongodb://a.example:27017,b.example:27017/db.coll",
        MongoSourceTask.createDefaultPartitionName(forward));
  }

  @Test
  void changedHostListWithoutReplicaSetStillChangesThePartition() {
    MongoSourceConfig original = config("mongodb://a.example:27017,b.example:27017");
    MongoSourceConfig edited = config("mongodb://a.example:27017,c.example:27017");

    assertNotEquals(
        MongoSourceTask.createDefaultPartitionName(original),
        MongoSourceTask.createDefaultPartitionName(edited));
  }

  @Test
  void readsLegacyOffsetWhenTheStablePartitionIsEmpty() {
    MongoSourceConfig cfg = config("mongodb://b.example:27017,a.example:27017/?replicaSet=rs0");
    Map<String, Object> stable = singletonMap("ns", "mongodb://rs0/db.coll");
    Map<String, Object> legacy =
        singletonMap("ns", "mongodb://a.example:27017,b.example:27017/db.coll");
    Map<String, Object> stored = singletonMap(ID_FIELD, "{\"_data\": \"resume-token\"}");
    OffsetStorageReader reader = mock(OffsetStorageReader.class);
    SourceTaskContext context = mock(SourceTaskContext.class);
    when(context.offsetStorageReader()).thenReturn(reader);
    when(reader.offset(stable)).thenReturn(null);
    when(reader.offset(legacy)).thenReturn(stored);

    assertEquals(stored, MongoSourceTask.getOffset(context, cfg));
    assertEquals(stable, MongoSourceTask.createPartitionMap(cfg));
    verify(reader).offset(legacy);
  }

  @Test
  void doesNotReadTheLegacyPartitionWhenTheStableOffsetExists() {
    MongoSourceConfig cfg = config("mongodb://a.example:27017,b.example:27017/?replicaSet=rs0");
    Map<String, Object> stable = singletonMap("ns", "mongodb://rs0/db.coll");
    Map<String, Object> legacy =
        singletonMap("ns", "mongodb://a.example:27017,b.example:27017/db.coll");
    Map<String, Object> stored = singletonMap(ID_FIELD, "{\"_data\": \"resume-token\"}");
    OffsetStorageReader reader = mock(OffsetStorageReader.class);
    SourceTaskContext context = mock(SourceTaskContext.class);
    when(context.offsetStorageReader()).thenReturn(reader);
    when(reader.offset(stable)).thenReturn(stored);

    assertEquals(stored, MongoSourceTask.getOffset(context, cfg));
    verify(reader, never()).offset(legacy);
  }

  @Test
  void customPartitionNameDoesNotFallBackToTheLegacyKey() {
    Map<String, String> props = props("mongodb://a.example:27017,b.example:27017/?replicaSet=rs0");
    props.put(OFFSET_PARTITION_NAME_CONFIG, "pinned");
    MongoSourceConfig cfg = new MongoSourceConfig(props);
    OffsetStorageReader reader = mock(OffsetStorageReader.class);
    SourceTaskContext context = mock(SourceTaskContext.class);
    when(context.offsetStorageReader()).thenReturn(reader);
    when(reader.offset(singletonMap("ns", "pinned"))).thenReturn(null);

    assertNull(MongoSourceTask.getOffset(context, cfg));
    verify(reader, times(1)).offset(any());
  }

  @Test
  void errorsToleranceDoesNotRecreateTheStreamWithoutTheResumeToken() {
    Map<String, String> props = props("mongodb://localhost:27017");
    props.put(DATABASE_CONFIG, "db");
    props.put(COLLECTION_CONFIG, "coll");
    props.put(ERRORS_TOLERANCE_CONFIG, "all");
    MongoSourceConfig cfg = new MongoSourceConfig(props);

    MongoClient client = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> documents = mock(MongoCollection.class);
    ChangeStreamIterable<Document> stream = mock(ChangeStreamIterable.class);
    MongoIterable<RawBsonDocument> raw = mock(MongoIterable.class);
    MongoChangeStreamCursor<RawBsonDocument> cursor = mock(MongoChangeStreamCursor.class);
    OffsetStorageReader reader = mock(OffsetStorageReader.class);
    SourceTaskContext context = mock(SourceTaskContext.class);
    when(context.offsetStorageReader()).thenReturn(reader);
    when(reader.offset(any())).thenReturn(singletonMap(ID_FIELD, "{\"_data\": \"8264\"}"));
    when(client.getDatabase("db")).thenReturn(database);
    when(database.getCollection("coll")).thenReturn(documents);
    when(documents.watch()).thenReturn(stream);
    when(stream.startAfter(any())).thenReturn(stream);
    when(stream.withDocumentClass(any())).thenReturn(cast(raw));
    when(raw.cursor()).thenReturn(cursor);
    when(cursor.getServerCursor()).thenReturn(new ServerCursor(1L, new ServerAddress()));
    when(cursor.tryNext()).thenThrow(new MongoException(286, "resume token was not found"));

    StartedMongoSourceTask task =
        new StartedMongoSourceTask(
            () -> context, cfg, client, null, new JmxStatisticsManager(false, "checkpoint-resume"));
    try {
      ConnectException failure = assertThrows(ConnectException.class, task::poll);
      assertTrue(failure.getMessage().contains("stored resume token"), failure.getMessage());
      verify(stream).startAfter(BsonDocument.parse("{\"_data\": \"8264\"}"));
      verify(stream, times(1)).startAfter(any());
      verify(raw, times(1)).cursor();
    } finally {
      task.close();
    }
  }

  @Test
  void invalidatedResumeTokenFailsInsteadOfRestartingFromNow() {
    Map<String, String> props = props("mongodb://localhost:27017");
    props.put(DATABASE_CONFIG, "db");
    props.put(COLLECTION_CONFIG, "coll");
    MongoSourceConfig cfg = new MongoSourceConfig(props);

    MongoClient client = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> documents = mock(MongoCollection.class);
    ChangeStreamIterable<Document> stream = mock(ChangeStreamIterable.class);
    MongoIterable<RawBsonDocument> raw = mock(MongoIterable.class);
    OffsetStorageReader reader = mock(OffsetStorageReader.class);
    SourceTaskContext context = mock(SourceTaskContext.class);
    when(context.offsetStorageReader()).thenReturn(reader);
    when(reader.offset(any())).thenReturn(singletonMap(ID_FIELD, "{\"_data\": \"8264\"}"));
    when(client.getDatabase("db")).thenReturn(database);
    when(database.getCollection("coll")).thenReturn(documents);
    when(documents.watch()).thenReturn(stream);
    when(stream.startAfter(any())).thenReturn(stream);
    when(stream.withDocumentClass(any())).thenReturn(cast(raw));
    BsonDocument response =
        new BsonDocument()
            .append("ok", new BsonDouble(0))
            .append("code", new BsonInt32(260))
            .append("errmsg", new BsonString("invalidated resume token"));
    when(raw.cursor()).thenThrow(new MongoCommandException(response, new ServerAddress()));

    JmxStatisticsManager statistics = new JmxStatisticsManager(false, "checkpoint-invalid");
    try {
      ConnectException failure =
          assertThrows(
              ConnectException.class,
              () -> new StartedMongoSourceTask(() -> context, cfg, client, null, statistics));
      assertTrue(failure.getMessage().contains("stored resume token"), failure.getMessage());
      verify(stream, times(1)).startAfter(any());
      verify(raw, times(1)).cursor();
    } finally {
      statistics.close();
    }
  }

  @Test
  void copyExistingWatchesBeforeTheSnapshotAggregate() throws InterruptedException {
    Map<String, String> props = props("mongodb://localhost:27017");
    props.put(DATABASE_CONFIG, "db");
    props.put(COLLECTION_CONFIG, "coll");
    props.put(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
    MongoSourceConfig cfg = new MongoSourceConfig(props);

    List<String> order = new ArrayList<>();
    CountDownLatch aggregateEntered = new CountDownLatch(1);
    MongoClient client = mock(MongoClient.class);
    MongoDatabase database = mock(MongoDatabase.class);
    MongoCollection<Document> documents = mock(MongoCollection.class);
    MongoCollection<RawBsonDocument> rawCollection = mock(MongoCollection.class);
    ChangeStreamIterable<Document> stream = mock(ChangeStreamIterable.class);
    MongoChangeStreamCursor<?> cursor = mock(MongoChangeStreamCursor.class);
    AggregateIterable<RawBsonDocument> aggregate = mock(AggregateIterable.class);
    when(client.getDatabase("db")).thenReturn(database);
    when(database.getCollection("coll")).thenReturn(documents);
    when(database.getCollection(eq("coll"), eq(RawBsonDocument.class))).thenReturn(rawCollection);
    when(documents.watch())
        .thenAnswer(
            invocation -> {
              order.add("watch");
              return stream;
            });
    doReturn(cursor).when(stream).cursor();
    when(cursor.tryNext()).thenReturn(null);
    when(cursor.getResumeToken()).thenReturn(new BsonDocument("_data", new BsonString("token")));
    when(rawCollection.aggregate(anyList()))
        .thenAnswer(
            invocation -> {
              order.add("aggregate");
              aggregateEntered.countDown();
              return aggregate;
            });
    when(aggregate.allowDiskUse(anyBoolean())).thenReturn(aggregate);

    MongoCopyDataManager copyDataManager = new MongoCopyDataManager(cfg, client, false);
    assertTrue(order.isEmpty(), order::toString);
    StartedMongoSourceTask task =
        new StartedMongoSourceTask(
            () -> null,
            cfg,
            client,
            copyDataManager,
            new JmxStatisticsManager(true, "checkpoint-copy"));
    try {
      assertTrue(aggregateEntered.await(5, TimeUnit.SECONDS));
      assertEquals(asList("watch", "aggregate"), order);
    } finally {
      task.close();
    }
  }

  private static MongoSourceConfig config(final String uri) {
    return new MongoSourceConfig(props(uri));
  }

  private static Map<String, String> props(final String uri) {
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTION_URI_CONFIG, uri);
    props.put(DATABASE_CONFIG, "db");
    props.put(COLLECTION_CONFIG, "coll");
    return props;
  }

  private static <T> T cast(final Object value) {
    return (T) value;
  }
}
