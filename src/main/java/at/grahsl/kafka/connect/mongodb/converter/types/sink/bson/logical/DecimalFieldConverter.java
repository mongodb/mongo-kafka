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

package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import java.math.BigDecimal;

public class DecimalFieldConverter extends SinkFieldConverter {

    public enum Format {
        DECIMAL128,         //needs MongoDB v3.4+
        LEGACYDOUBLE        //results in double approximation
    }

    private Format format;

    public DecimalFieldConverter() {
        super(Decimal.schema(0));
        this.format = Format.DECIMAL128;
    }

    public DecimalFieldConverter(Format format) {
        super(Decimal.schema(0));
        this.format = format;
    }

    @Override
    public BsonValue toBson(Object data) {

        if(data instanceof BigDecimal) {
            if(format.equals(Format.DECIMAL128))
                return new BsonDecimal128(new Decimal128((BigDecimal)data));

            if(format.equals(Format.LEGACYDOUBLE))
                return new BsonDouble(((BigDecimal)data).doubleValue());
        }

        throw new DataException("error: decimal conversion not possible when data is"
                        + " of type "+data.getClass().getName() + " and format is "+format);

    }
}
