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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonNull;
import org.bson.BsonValue;

public abstract class SinkFieldConverter extends FieldConverter {

    public SinkFieldConverter(Schema schema) {
        super(schema);
    }

    public abstract BsonValue toBson(Object data);

    public BsonValue toBson(Object data, Schema fieldSchema) {
        if(!fieldSchema.isOptional()) {

            if(data == null)
                throw new DataException("error: schema not optional but data was null");

            return toBson(data);
        }

        if(data != null) {
            return toBson(data);
        }

        if(fieldSchema.defaultValue() != null) {
            return toBson(fieldSchema.defaultValue());
        }

        return new BsonNull();
    }

}
