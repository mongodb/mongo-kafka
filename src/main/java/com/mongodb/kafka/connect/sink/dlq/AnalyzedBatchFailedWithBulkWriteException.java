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
package com.mongodb.kafka.connect.sink.dlq;

import static com.mongodb.kafka.connect.util.Assertions.assertNotNull;
import static com.mongodb.kafka.connect.util.Assertions.assertTrue;
import static java.util.Collections.singletonList;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.lang.Nullable;

/**
 * This class allows to conveniently {@linkplain #log() log} and {@linkplain #report() report} to
 * the DLQ a {@linkplain MongoCollection#bulkWrite(List, BulkWriteOptions) batch} failed with {@link
 * MongoBulkWriteException}.
 */
public final class AnalyzedBatchFailedWithBulkWriteException {
  private final List<SinkRecord> batch;
  private final MongoBulkWriteException e;
  private final ErrantRecordReporter errorReporter;
  private final Logger logger;
  private final Map<Integer, Map.Entry<SinkRecord, WriteException>> recordsFailedWithWriteError =
      new HashMap<>();
  private final Map<Integer, SinkRecord> recordsFailedWithWriteConcernError = new HashMap<>();
  private final Map<Integer, SinkRecord> skippedRecords = new HashMap<>();
  @Nullable private final WriteConcernException writeConcernException;
  private final WriteSkippedException writeSkippedException = new WriteSkippedException();

  public AnalyzedBatchFailedWithBulkWriteException(
      final List<SinkRecord> batch,
      final MongoBulkWriteException e,
      final ErrantRecordReporter errorReporter,
      final Logger logger) {
    this.batch = batch;
    this.e = e;
    this.errorReporter = errorReporter;
    this.logger = logger;
    WriteConcernError writeConcernError = e.getWriteConcernError();
    writeConcernException =
        writeConcernError == null ? null : new WriteConcernException(writeConcernError);
    analyze();
  }

  private void analyze() {
    List<BulkWriteError> writeErrors = e.getWriteErrors();
    WriteConcernError writeConcernError = e.getWriteConcernError();
    if (writeErrors.isEmpty()) {
      assertNotNull(writeConcernError);
      for (int i = 0; i < batch.size(); i++) {
        recordsFailedWithWriteConcernError.put(i, batch.get(i));
      }
    } else {
      assertTrue(writeErrors.size() == 1);
      BulkWriteError writeError = writeErrors.get(0);
      int writeErrorIdx = writeError.getIndex();
      recordsFailedWithWriteError.put(
          writeErrorIdx,
          new AbstractMap.SimpleImmutableEntry<>(
              batch.get(writeErrorIdx), new WriteException(writeError)));
      for (int i = writeErrorIdx + 1; i < batch.size(); i++) {
        skippedRecords.put(i, batch.get(i));
      }
      if (writeConcernError != null) {
        for (int i = 0; i < writeErrorIdx; i++) {
          recordsFailedWithWriteConcernError.put(i, batch.get(i));
        }
      }
    }
  }

  public void log() {
    if (!recordsFailedWithWriteError.isEmpty()) {
      recordsFailedWithWriteError.forEach(
          (i, recordFailedWithWriteError) ->
              logger.log(
                  singletonList(recordFailedWithWriteError.getKey()),
                  recordFailedWithWriteError.getValue()));
    }
    if (!recordsFailedWithWriteConcernError.isEmpty()) {
      logger.log(recordsFailedWithWriteConcernError.values(), assertNotNull(writeConcernException));
    }
    if (!skippedRecords.isEmpty()) {
      logger.log(skippedRecords.values(), writeSkippedException);
    }
    logger.log(null, e);
  }

  /**
   * {@linkplain #errorReporter Reports} records in the batch that either definitely have not been
   * written, or may not have been written. While it is unclear whether the order of reported
   * records in the DLQ is the same as the order in which they are reported, we still report them in
   * the order they are present in the batch just in case because it is trivial to do.
   */
  public void report() {
    for (int i = 0; i < batch.size(); i++) {
      Map.Entry<SinkRecord, WriteException> recordFailedWithWriteError =
          recordsFailedWithWriteError.get(i);
      if (recordFailedWithWriteError != null) {
        errorReporter.report(
            recordFailedWithWriteError.getKey(), recordFailedWithWriteError.getValue());
        continue;
      }
      SinkRecord recordFailedWithWriteConcernError = recordsFailedWithWriteConcernError.get(i);
      if (recordFailedWithWriteConcernError != null) {
        errorReporter.report(
            recordFailedWithWriteConcernError, assertNotNull(writeConcernException));
        continue;
      }
      SinkRecord skippedRecord = skippedRecords.get(i);
      if (skippedRecord != null) {
        errorReporter.report(skippedRecord, writeSkippedException);
      }
    }
  }

  public interface Logger {
    void log(Collection<SinkRecord> records, RuntimeException e);
  }
}
