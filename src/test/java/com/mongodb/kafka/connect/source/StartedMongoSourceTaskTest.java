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

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.bson.BsonTimestamp;
import org.bson.Document;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.mongodb.kafka.connect.source.statistics.JmxStatisticsManager;

final class StartedMongoSourceTaskTest {
  @Nested
  final class ChangeStreamIterableOptionsTest {
    private final Map<String, String> properties = new HashMap<>();
    private StartedMongoSourceTask task;

    @BeforeEach
    void setUp() {
      properties.clear();
    }

    @AfterEach
    void tearDown() {
      if (task != null) {
        task.close();
      }
    }

    @Test
    void fullDocumentBeforeChange() {
      FullDocumentBeforeChange expected = FullDocumentBeforeChange.WHEN_AVAILABLE;
      properties.put(MongoSourceConfig.FULL_DOCUMENT_BEFORE_CHANGE_CONFIG, expected.getValue());
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.watch()).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      task.poll();
      ArgumentCaptor<FullDocumentBeforeChange> argCaptor =
          ArgumentCaptor.forClass(FullDocumentBeforeChange.class);
      verify(changeStreamIterable, atLeastOnce()).fullDocumentBeforeChange(argCaptor.capture());
      List<FullDocumentBeforeChange> capturedArgs = argCaptor.getAllValues();
      assertTrue(capturedArgs.stream().allMatch(v -> v.equals(expected)), capturedArgs::toString);
    }

    @Test
    void startAtOperationTime() {
      int expectedEpochSeconds = 123;
      BsonTimestamp expected = new BsonTimestamp(expectedEpochSeconds, 0);
      properties.put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue());
      properties.put(
          MongoSourceConfig.STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG,
          String.valueOf(expectedEpochSeconds));
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.watch()).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      task.poll();
      ArgumentCaptor<BsonTimestamp> argCaptor = ArgumentCaptor.forClass(BsonTimestamp.class);
      verify(changeStreamIterable, atLeastOnce()).startAtOperationTime(argCaptor.capture());
      List<BsonTimestamp> capturedArgs = argCaptor.getAllValues();
      assertTrue(capturedArgs.stream().allMatch(v -> v.equals(expected)), capturedArgs::toString);
    }

    @Test
    void splitLargeEventDisabledByDefault() {
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.watch()).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      task.poll();
      // Verify that watch() was called without a pipeline (no split stage added)
      verify(client, atLeastOnce()).watch();
      // Verify that watch(List) was NEVER called (no pipeline should be passed)
      verify(client, never()).watch(any(List.class));
    }

    @Test
    void splitLargeEventEnabled() {
      properties.put(MongoSourceConfig.SPLIT_LARGE_EVENT_CONFIG, "true");
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.watch(any(List.class))).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      task.poll();
      // Verify that watch() was called with a pipeline containing the split stage
      ArgumentCaptor<List<Document>> pipelineCaptor = ArgumentCaptor.forClass(List.class);
      verify(client, atLeastOnce()).watch(pipelineCaptor.capture());
      List<Document> pipeline = pipelineCaptor.getValue();
      assertEquals(1, pipeline.size());
      assertEquals(new Document("$changeStreamSplitLargeEvent", new Document()), pipeline.get(0));
    }

    @Test
    void splitLargeEventWithExistingPipeline() {
      properties.put(MongoSourceConfig.SPLIT_LARGE_EVENT_CONFIG, "true");
      properties.put(
          MongoSourceConfig.PIPELINE_CONFIG, "[{\"$match\": {\"operationType\": \"insert\"}}]");
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.watch(any(List.class))).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      task.poll();
      // Verify that watch() was called with a pipeline containing user pipeline first, then split
      // stage last
      ArgumentCaptor<List<Document>> pipelineCaptor = ArgumentCaptor.forClass(List.class);
      verify(client, atLeastOnce()).watch(pipelineCaptor.capture());
      List<Document> pipeline = pipelineCaptor.getValue();
      assertEquals(2, pipeline.size());
      assertEquals(
          new Document("$match", new Document("operationType", "insert")), pipeline.get(0));
      assertEquals(new Document("$changeStreamSplitLargeEvent", new Document()), pipeline.get(1));
    }

    @Test
    void splitLargeEventWithComplexMultiStagePipeline() {
      properties.put(MongoSourceConfig.SPLIT_LARGE_EVENT_CONFIG, "true");
      // Test with a complex multi-stage pipeline
      properties.put(
          MongoSourceConfig.PIPELINE_CONFIG,
          "[{\"$match\": {\"operationType\": \"insert\"}}, "
              + "{\"$project\": {\"_id\": 1, \"fullDocument\": 1}}, "
              + "{\"$limit\": 100}]");
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.watch(any(List.class))).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      task.poll();
      // Verify that watch() was called with all user pipeline stages first, then split stage last
      ArgumentCaptor<List<Document>> pipelineCaptor = ArgumentCaptor.forClass(List.class);
      verify(client, atLeastOnce()).watch(pipelineCaptor.capture());
      List<Document> pipeline = pipelineCaptor.getValue();
      assertEquals(4, pipeline.size(), "Should have 3 user pipeline stages + split stage");
      // Verify user pipeline stages come first in order
      assertEquals(
          new Document("$match", new Document("operationType", "insert")),
          pipeline.get(0),
          "First user stage should be $match");
      assertEquals(
          new Document("$project", new Document("_id", 1).append("fullDocument", 1)),
          pipeline.get(1),
          "Second user stage should be $project");
      assertEquals(
          new Document("$limit", 100), pipeline.get(2), "Third user stage should be $limit");
      // Verify split stage is last
      assertEquals(
          new Document("$changeStreamSplitLargeEvent", new Document()),
          pipeline.get(3),
          "Split stage should be last");
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(final Object o) {
    return (T) o;
  }
}
