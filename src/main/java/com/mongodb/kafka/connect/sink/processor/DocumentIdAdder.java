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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.sink.processor;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ID_FIELD;

import org.apache.kafka.connect.sink.SinkRecord;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.id.strategy.IdStrategy;

public class DocumentIdAdder extends PostProcessor {
    private final IdStrategy idStrategy;

    public DocumentIdAdder(final MongoSinkTopicConfig config) {
        super(config);
        this.idStrategy = config.getIdStrategy();
    }

    @Override
    public void process(final SinkDocument doc, final SinkRecord orig) {
        doc.getValueDoc().ifPresent(vd -> vd.append(ID_FIELD, idStrategy.generateId(doc, orig)));
    }

}
