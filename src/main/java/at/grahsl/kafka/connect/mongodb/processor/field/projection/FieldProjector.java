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

package at.grahsl.kafka.connect.mongodb.processor.field.projection;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.processor.PostProcessor;
import org.bson.BsonDocument;

import java.util.Set;

public abstract class FieldProjector extends PostProcessor {

    public static final String SINGLE_WILDCARD = "*";
    public static final String DOUBLE_WILDCARD = "**";
    public static final String SUB_FIELD_DOT_SEPARATOR = ".";

    protected Set<String> fields;
    
    public FieldProjector(MongoDbSinkConnectorConfig config,String collection) {
        super(config,collection);
    }

    protected abstract void doProjection(String field, BsonDocument doc);

}
