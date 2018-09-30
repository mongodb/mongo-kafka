package at.grahsl.kafka.connect.mongodb;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

public class CollectionAwareConfig extends AbstractConfig {

    //NOTE: the merging of values() and originals() is a workaround
    //in order to allow for properties not being given in the
    //ConfigDef at compile time to be picked up and available as well...
    private final Map<String, Object> collectionAwareSettings;

    public CollectionAwareConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        super(definition, originals, doLog);
        collectionAwareSettings = new HashMap<>(256);
        collectionAwareSettings.putAll(values());
        collectionAwareSettings.putAll(originals());
    }

    public CollectionAwareConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
        collectionAwareSettings = new HashMap<>(256);
        collectionAwareSettings.putAll(values());
        collectionAwareSettings.putAll(originals());
    }

    protected Object get(String property, String collection) {
        String fullProperty = property+"."+collection;
        if(collectionAwareSettings.containsKey(fullProperty)) {
           return collectionAwareSettings.get(fullProperty);
        }
        return collectionAwareSettings.get(property);
    }

    public String getString(String property, String collection) {
        if(collection == null || collection.isEmpty()) {
            return (String) get(property);
        }
        return (String) get(property,collection);
    }

    //NOTE: in the this topic aware map, everything is currently stored as
    //type String so direct casting won't work which is why the
    //*.parse*(String value) methods are to be used for now.
    public Boolean getBoolean(String property, String collection) {
        Object obj;

        if(collection == null || collection.isEmpty()) {
            obj = get(property);
        } else {
            obj = get(property,collection);
        }

        if(obj instanceof Boolean)
            return (Boolean) obj;

        if(obj instanceof String)
            return Boolean.parseBoolean((String)obj);

        throw new ConfigException("error: unsupported property type for '"+obj+"' where Boolean expected");
    }

    public Integer getInt(String property, String collection) {

        Object obj;

        if(collection == null || collection.isEmpty()) {
            obj = get(property);
        } else {
            obj = get(property,collection);
        }

        if(obj instanceof Integer)
            return (Integer) obj;

        if(obj instanceof String)
            return Integer.parseInt((String)obj);

        throw new ConfigException("error: unsupported property type for '"+obj+"' where Integer expected");
    }

}
