package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;

public abstract class FieldConverter {

    private final Schema schema;

    public FieldConverter(Schema schema) {
        this.schema = schema;
    }

    public Schema getSchema() { return schema; }

}
