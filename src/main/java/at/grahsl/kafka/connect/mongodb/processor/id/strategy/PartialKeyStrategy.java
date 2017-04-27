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

package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.field.projection.FieldProjector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class PartialKeyStrategy implements IdStrategy {

    private FieldProjector fieldProjector;

    public PartialKeyStrategy(FieldProjector fieldProjector) {
        this.fieldProjector = fieldProjector;
    }

    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {

        fieldProjector.process(doc,orig);
        //NOTE: If there is no key doc present the strategy
        //simply returns an empty BSON document per default.
        return doc.getKeyDoc().orElseGet(() -> new BsonDocument());

    }

}
