package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;

import java.util.Map;

public class RenameByMapping extends Renamer {

    private Map<String,String> fieldMappings;

    public RenameByMapping(MongoDbSinkConnectorConfig config) {
        super(config);
        this.fieldMappings = config.parseRenameFieldnameMappings();
    }

    public RenameByMapping(MongoDbSinkConnectorConfig config,
                                Map<String, String> fieldMappings) {
        super(config);
        this.fieldMappings = fieldMappings;
    }

    @Override
    protected boolean isActive() {
        return !fieldMappings.isEmpty();
    }

    protected String renamed(String path, String name) {
        String newName = fieldMappings.get(path+SUB_FIELD_DOT_SEPARATOR+name);
        return newName != null ? newName : name;
    }

}
