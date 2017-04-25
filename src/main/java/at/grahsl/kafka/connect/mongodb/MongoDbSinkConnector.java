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

package at.grahsl.kafka.connect.mongodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDbSinkConnector extends SinkConnector {

  private Map<String, String> settings;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    settings = map;
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
