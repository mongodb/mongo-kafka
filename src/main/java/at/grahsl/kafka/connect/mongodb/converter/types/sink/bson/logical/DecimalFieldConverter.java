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

        if(format.equals(Format.DECIMAL128))
            return new BsonDecimal128(new Decimal128((BigDecimal)data));

        if(format.equals(Format.LEGACYDOUBLE))
            return new BsonDouble(((BigDecimal)data).doubleValue());

        throw new DataException("error: decimal conversion using format "
                + format + " not supported");

    }
}
