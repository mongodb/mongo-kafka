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

package at.grahsl.kafka.connect.mongodb.converter;

import org.bson.BsonDocument;

import java.util.Optional;

public class SinkDocument {

    private final Optional<BsonDocument> keyDoc;
    private final Optional<BsonDocument> valueDoc;

    public SinkDocument(BsonDocument keyDoc, BsonDocument valueDoc) {
        this.keyDoc = Optional.ofNullable(keyDoc);
        this.valueDoc = Optional.ofNullable(valueDoc);
    }

    public Optional<BsonDocument> getKeyDoc() {
        return keyDoc;
    }

    public Optional<BsonDocument> getValueDoc() {
        return valueDoc;
    }

    public SinkDocument clone() {
        BsonDocument kd = keyDoc.isPresent() ? keyDoc.get().clone() : null;
        BsonDocument vd = valueDoc.isPresent() ? valueDoc.get().clone() : null;
        return new SinkDocument(kd,vd);
    }

}
