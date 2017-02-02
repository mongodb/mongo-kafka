package at.grahsl.kafka.connect.mongodb;

import com.github.jcustenborder.kafka.connect.utils.config.MarkdownFormatter;
import com.mongodb.MongoClientURI;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MongoDbSinkConnectorConfigTest {

    public static final String CLIENT_URI_DEFAULT_SETTINGS =
            "mongodb://localhost:27017/kafkaconnect?w=1";
    public static final String CLIENT_URI_AUTH_SETTINGS =
            "mongodb://hanspeter:secret@localhost:27017/kafkaconnect?authSource=admin&w=1";

    @Test
    public void doc() {
        //System.out.println(MongoDbSinkConnectorConfig.conf().toRst());
        System.out.println(MarkdownFormatter.toMarkdown(MongoDbSinkConnectorConfig.conf()));
    }

    @Test
    public void buildClientUriWithDefaultSettings() {

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(new HashMap<>());

        MongoClientURI uri = cfg.buildClientURI();

        assertEquals("wrong connection uri", CLIENT_URI_DEFAULT_SETTINGS, uri.toString());

    }

    @Test
    public void buildClientUriWithAuthSettings() {

        Map<String, String> map = new HashMap<>();
        map.put(MongoDbSinkConnectorConfig.MONGODB_AUTH_ACTIVE_CONF, "true");
        map.put(MongoDbSinkConnectorConfig.MONGODB_USERNAME_CONF, "hanspeter");
        map.put(MongoDbSinkConnectorConfig.MONGODB_PASSWORD_CONF, "secret");

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(map);

        MongoClientURI uri = cfg.buildClientURI();

        assertEquals("wrong connection uri", CLIENT_URI_AUTH_SETTINGS, uri.toString());

    }

}
