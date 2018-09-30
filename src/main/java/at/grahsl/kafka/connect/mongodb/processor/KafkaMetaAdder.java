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

package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonInt64;
import org.bson.BsonString;

public class KafkaMetaAdder extends PostProcessor {

    public static final String KAFKA_META_DATA = "topic-partition-offset";

    public KafkaMetaAdder(MongoDbSinkConnectorConfig config,String collection) {
        super(config,collection);
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {

        doc.getValueDoc().ifPresent(vd -> {
            vd.put(KAFKA_META_DATA, new BsonString(orig.topic()
                    + "-" + orig.kafkaPartition() + "-" + orig.kafkaOffset()));
            vd.put(orig.timestampType().name(), new BsonInt64(orig.timestamp()));
        });

        getNext().ifPresent(pp -> pp.process(doc, orig));
    }

}
