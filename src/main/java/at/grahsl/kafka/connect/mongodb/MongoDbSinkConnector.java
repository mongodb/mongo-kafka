package at.grahsl.kafka.connect.mongodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDbSinkConnector extends SinkConnector {

  private static Logger logger = LoggerFactory.getLogger(MongoDbSinkConnector.class);

  private MongoDbSinkConnectorConfig config;
  private Map<String, String> settings;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    settings = map;
    config = new MongoDbSinkConnectorConfig(map);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MongoDbSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {

    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(settings);
    }

    return taskConfigs;

  }

  @Override
  public void stop() {
    //TODO: what's necessary to stop the connector
  }

  @Override
  public ConfigDef config() {
    return MongoDbSinkConnectorConfig.conf();
  }
}
