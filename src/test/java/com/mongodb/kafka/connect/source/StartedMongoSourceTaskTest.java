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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.bson.BsonTimestamp;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.mongodb.kafka.connect.source.statistics.JmxStatisticsManager;

final class StartedMongoSourceTaskTest {
  static final class ChangeStreamIterableOptionsTest {
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
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(final Object o) {
    return (T) o;
  }
}
