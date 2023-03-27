/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */
package com.mongodb.kafka.connect.source.statistics;

import java.util.Map;
import javax.management.ObjectName;

import com.mongodb.annotations.ThreadSafe;

import com.mongodb.kafka.connect.util.jmx.SourceTaskStatistics;
import com.mongodb.kafka.connect.util.jmx.internal.CombinedMongoMBean;
import com.mongodb.kafka.connect.util.jmx.internal.MBeanServerUtils;

@ThreadSafe
public final class JmxStatisticsManager implements StatisticsManager {
  private static final String COPY_BEAN = "source-task-copy-existing";
  private static final String STREAM_BEAN = "source-task-change-stream";
  private static final String COMBINED_BEAN = "source-task";

  private final SourceTaskStatistics copyStatistics;
  private final SourceTaskStatistics streamStatistics;
  private final CombinedMongoMBean combinedStatistics;
  private volatile SourceTaskStatistics currentStatistics;

  public JmxStatisticsManager(final boolean startWithCopyStatistics, final String connectorName) {
    copyStatistics = new SourceTaskStatistics(getMBeanName(COPY_BEAN, connectorName));
    streamStatistics = new SourceTaskStatistics(getMBeanName(STREAM_BEAN, connectorName));
    combinedStatistics =
        new CombinedMongoMBean(
            getMBeanName(COMBINED_BEAN, connectorName), copyStatistics, streamStatistics);
    currentStatistics = startWithCopyStatistics ? copyStatistics : streamStatistics;
    copyStatistics.register();
    streamStatistics.register();
    combinedStatistics.register();
  }

  @Override
  public SourceTaskStatistics currentStatistics() {
    return currentStatistics;
  }

  @Override
  public void switchToStreamStatistics() {
    currentStatistics = streamStatistics;
  }

  @Override
  public void close() {
    copyStatistics.unregister();
    streamStatistics.unregister();
    combinedStatistics.unregister();
  }

  private static String getMBeanName(final String mBean, final String connectorName) {
    String id = MBeanServerUtils.taskIdFromCurrentThread();
    return "com.mongodb.kafka.connect:type=source-task-metrics,connector="
        + connectorName
        + ",task="
        + mBean
        + "-"
        + id;
  }

  public static String getConnectorName(final Map<String, String> props) {
    String originalName = props.getOrDefault("name", "unknown");
    String quotedName = ObjectName.quote(originalName);
    if (quotedName.substring(1, quotedName.length() - 1).equals(originalName)) {
      return originalName;
    }
    return quotedName;
  }
}
