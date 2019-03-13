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

package com.mongodb.kafka.connect.processor;

import java.util.Optional;

import org.apache.kafka.connect.sink.SinkRecord;

import com.mongodb.kafka.connect.MongoSinkConnectorConfig;
import com.mongodb.kafka.connect.converter.SinkDocument;

public abstract class PostProcessor {

    private final MongoSinkConnectorConfig config;
    private Optional<PostProcessor> next = Optional.empty();
    private final String collection;

    public PostProcessor(final MongoSinkConnectorConfig config, final String collection) {
        this.config = config;
        this.collection = collection;
    }

    public PostProcessor chain(final PostProcessor next) {
        // intentionally throws NPE here if someone
        // tries to be 'smart' by chaining with null
        this.next = Optional.of(next);
        return this.next.get();
    }

    public abstract void process(SinkDocument doc, SinkRecord orig);

    public MongoSinkConnectorConfig getConfig() {
        return this.config;
    }

    public Optional<PostProcessor> getNext() {
        return this.next;
    }

    public String getCollection() {
        return this.collection;
    }

}
