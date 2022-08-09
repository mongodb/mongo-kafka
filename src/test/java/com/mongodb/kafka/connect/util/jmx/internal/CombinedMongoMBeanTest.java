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
 */

package com.mongodb.kafka.connect.util.jmx.internal;

import static com.mongodb.kafka.connect.util.jmx.internal.MongoMBeanTest.getAssertedAttributeSupplier;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CombinedMongoMBeanTest {
  private List<Long> asList(final Number... n) {
    return Arrays.stream(n).map(Number::longValue).collect(Collectors.toList());
  }

  @Test
  @DisplayName("Should be visible after registration and not after un-registration")
  void testCombinedMBeanRegistration() throws Exception {
    String query = "com.mongodb.test.*:*";
    String name = "com.mongodb.test.abc:key1=value1";

    CombinedMongoMBean bean = new CombinedMongoMBean(name, new MongoMBean(""), new MongoMBean(""));

    bean.register();
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> results = mBeanServer.queryNames(new ObjectName(query), null);
    List<String> names = results.stream().map(ObjectName::toString).collect(Collectors.toList());
    assertEquals(1, names.size());
    assertEquals(bean.getName(), names.get(0));

    bean.unregister();
    assertEquals(0, mBeanServer.queryNames(new ObjectName(query), null).size());
  }

  @Test
  @DisplayName("Should correctly combine metric values")
  void testCombineMetrics() {
    MongoMBean bean1 = new MongoMBean("name1");
    TotalMetric totalMetric1 = bean1.registerTotal("total");
    LatestMetric latestMetric1 = bean1.registerLatest("latest");
    AdditiveMetric timeMetric1 = bean1.registerMs("time");

    MongoMBean bean2 = new MongoMBean("name1");
    TotalMetric totalMetric2 = bean2.registerTotal("total");
    LatestMetric latestMetric2 = bean2.registerLatest("latest");
    AdditiveMetric timeMetric2 = bean2.registerMs("time");

    CombinedMongoMBean bean3 = new CombinedMongoMBean("bean3", bean1, bean2);

    Supplier<List<Long>> bean1Supplier = getAssertedAttributeSupplier(bean1);
    Supplier<List<Long>> bean2Supplier = getAssertedAttributeSupplier(bean2);
    Supplier<List<Long>> bean3Supplier = getAssertedAttributeSupplier(bean3);

    // test sampling each type of metric
    assertEquals(asList(0, 0, 0, 0, 0, 0, 0, 0, 0), bean1Supplier.get());
    assertEquals(asList(0, 0, 0, 0, 0, 0, 0, 0, 0), bean2Supplier.get());
    assertEquals(asList(0, 0, 0, 0, 0, 0, 0, 0, 0), bean3Supplier.get());

    totalMetric1.sample(3);
    latestMetric1.sample(3);
    timeMetric1.sample(3);

    assertEquals(asList(3, 3, 1, 3, 1, 0, 0, 0, 0), bean1Supplier.get());
    assertEquals(asList(0, 0, 0, 0, 0, 0, 0, 0, 0), bean2Supplier.get());
    assertEquals(asList(3, 3, 1, 3, 1, 0, 0, 0, 0), bean3Supplier.get());

    totalMetric2.sample(2);
    latestMetric2.sample(2);
    timeMetric2.sample(2);

    assertEquals(asList(3, 3, 1, 3, 1, 0, 0, 0, 0), bean1Supplier.get());
    assertEquals(asList(2, 2, 1, 2, 1, 0, 0, 0, 0), bean2Supplier.get());
    assertEquals(asList(5, 2, 2, 5, 2, 0, 0, 0, 0), bean3Supplier.get());
  }
}
