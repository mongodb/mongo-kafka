/*
 * Copyright (c) 2018. Hans-Peter Grahsl (grahslhp@gmail.com)
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

package at.grahsl.kafka.connect.mongodb;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;

public class MongoDbSinkRecordBatches {

    private int batchSize;
    private int currentBatch = 0;
    private List<List<SinkRecord>> bufferedBatches = new ArrayList<>();

    public MongoDbSinkRecordBatches(int batchSize, int records) {
        this.batchSize = batchSize;
        bufferedBatches.add(batchSize > 0 ? new ArrayList<>(batchSize) : new ArrayList<>(records));
    }

    public void buffer(SinkRecord record) {
        if(batchSize > 0) {
            if(bufferedBatches.get(currentBatch).size() < batchSize) {
                bufferedBatches.get(currentBatch).add(record);
            } else {
                bufferedBatches.add(new ArrayList<>(batchSize));
                bufferedBatches.get(++currentBatch).add(record);
            }
        } else {
            bufferedBatches.get(0).add(record);
        }
    }

    public List<List<SinkRecord>> getBufferedBatches() {
        return bufferedBatches;
    }

}
