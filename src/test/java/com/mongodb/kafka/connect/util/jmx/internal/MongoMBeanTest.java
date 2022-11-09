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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.management.Attribute;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.collection.mutable.StringBuilder;

public class MongoMBeanTest {
  private List<Long> asList(final Number... n) {
    return Arrays.stream(n).map(Number::longValue).collect(Collectors.toList());
  }

  @Test
  @DisplayName("Should be visible after registration and not after un-registration")
  void testMBeanRegistration() throws Exception {
    String query = "com.mongodb.test.*:*";
    String name = "com.mongodb.test.abc:key1=value1";

    MongoMBean bean = new MongoMBean(name);

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
  @DisplayName("Should register duplicates under different names")
  void testMBeanDuplicateRegistration() throws Exception {
    String query = "com.mongodb.test.*:*";
    String name = "com.mongodb.test.abc:key1=value1";

    MBeanServerUtils.NEXT_ID.set(0);

    MongoMBean bean1 = new MongoMBean(name);
    bean1.register();
    MongoMBean bean2 = new MongoMBean(name);
    bean2.register();

    assertEquals(name, bean1.getName());
    assertEquals(name + "-v0", bean2.getName());

    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> results = mBeanServer.queryNames(new ObjectName(query), null);
    Set<String> names = results.stream().map(ObjectName::toString).collect(Collectors.toSet());
    assertEquals(new HashSet<>(Arrays.asList(name, name + "-v0")), names);

    bean1.unregister();
    bean2.unregister();
  }

  @Test
  @DisplayName("Should sample registered metrics and emit them in correct order")
  void testMetricSampling() {
    // create the bean
    MongoMBean bean = new MongoMBean("name");
    assertEquals("name", bean.getName());

    // register metrics in the bean
    TotalMetric totalMetric = bean.registerTotal("total");
    LatestMetric latestMetric = bean.registerLatest("latest");
    AdditiveMetric timeMetric = bean.registerMs("time");

    // emit metrics in order of registration
    StringBuilder sb = new StringBuilder();
    bean.emit(v -> sb.append(v.getName()).append("/"));
    assertEquals(
        "total/latest/time/time-duration-ms/"
            + "time-duration-over-1-ms/time-duration-over-10-ms/time-duration-over-100-ms/"
            + "time-duration-over-1000-ms/time-duration-over-10000-ms/",
        sb.toString());

    // and via dynamic mbean getInfo
    StringBuilder sb2 = new StringBuilder();
    Arrays.stream(bean.getMBeanInfo().getAttributes()).forEach(v -> sb2.append(v.getName() + "/"));
    assertEquals(sb.toString(), sb2.toString());

    // supply metric values in order
    Supplier<List<Long>> metricSupplier = getAssertedMetricSupplier(bean);

    // test sampling each type of metric
    // starts at 0
    assertEquals(asList(0, 0, 0, 0, 0, 0, 0, 0, 0), metricSupplier.get());

    // total
    totalMetric.sample(3);
    assertEquals(asList(3, 0, 0, 0, 0, 0, 0, 0, 0), metricSupplier.get());
    totalMetric.sample(1);
    assertEquals(asList(4, 0, 0, 0, 0, 0, 0, 0, 0), metricSupplier.get());

    // overflow
    totalMetric.sample(Long.MAX_VALUE);
    totalMetric.sample(1); // overflows
    assertEquals(asList(Long.MIN_VALUE + 4, 0, 0, 0, 0, 0, 0, 0, 0), metricSupplier.get());
    totalMetric.sample(-(Long.MIN_VALUE + 1));
    totalMetric.sample(1);
    assertEquals(asList(4, 0, 0, 0, 0, 0, 0, 0, 0), metricSupplier.get());

    // latest
    latestMetric.sample(3);
    assertEquals(asList(4, 3, 0, 0, 0, 0, 0, 0, 0), metricSupplier.get());
    latestMetric.sample(-2);
    assertEquals(asList(4, -2, 0, 0, 0, 0, 0, 0, 0), metricSupplier.get());
    latestMetric.sample(5);
    assertEquals(asList(4, 5, 0, 0, 0, 0, 0, 0, 0), metricSupplier.get());

    // limits
    timeMetric.sample(0);
    assertEquals(asList(4, 5, 1, 0, 0, 0, 0, 0, 0), metricSupplier.get());
    timeMetric.sample(5);
    assertEquals(asList(4, 5, 2, 5, 1, 0, 0, 0, 0), metricSupplier.get());
    timeMetric.sample(50);
    assertEquals(asList(4, 5, 3, 55, 2, 1, 0, 0, 0), metricSupplier.get());
    timeMetric.sample(500);
    assertEquals(asList(4, 5, 4, 555, 3, 2, 1, 0, 0), metricSupplier.get());
    timeMetric.sample(5000);
    assertEquals(asList(4, 5, 5, 5555, 4, 3, 2, 1, 0), metricSupplier.get());
    timeMetric.sample(50000);
    assertEquals(asList(4, 5, 6, 55555, 5, 4, 3, 2, 1), metricSupplier.get());

    // limit boundary
    timeMetric.sample(9999);
    assertEquals(asList(4, 5, 7, 65554, 6, 5, 4, 3, 1), metricSupplier.get());
    timeMetric.sample(10000);
    assertEquals(asList(4, 5, 8, 75554, 7, 6, 5, 4, 1), metricSupplier.get());
    timeMetric.sample(10001); // over
    assertEquals(asList(4, 5, 9, 85555, 8, 7, 6, 5, 2), metricSupplier.get());

    // toJSON
    assertEquals(
        "{\"total\": 4, "
            + "\"latest\": 5, "
            + "\"time\": 9, "
            + "\"time-duration-ms\": 85555, "
            + "\"time-duration-over-1-ms\": 8, "
            + "\"time-duration-over-10-ms\": 7, "
            + "\"time-duration-over-100-ms\": 6, "
            + "\"time-duration-over-1000-ms\": 5, "
            + "\"time-duration-over-10000-ms\": 2}",
        bean.toJSON());
  }

  @Test
  @DisplayName("Should not allow duplicate metric registrations")
  void testDuplicateMetricsRegistration() {
    MongoMBean bean = new MongoMBean("name");
    bean.registerTotal("total");
    assertThrows(IllegalArgumentException.class, () -> bean.registerTotal("total"));
  }

  @NotNull
  public static Supplier<List<Long>> getAssertedMetricSupplier(final MongoMBean bean) {
    List<Supplier<Long>> supplierList = new ArrayList<>();
    bean.emit(v -> supplierList.add(v::get));
    Supplier<List<Long>> listSupplier =
        () -> supplierList.stream().map(Supplier::get).collect(Collectors.toList());
    assertEquals(listSupplier.get(), getAssertedAttributeSupplier(bean).get());
    return listSupplier;
  }

  @NotNull
  public static Supplier<List<Long>> getAssertedAttributeSupplier(final DynamicMBean bean) {
    List<String> names =
        Arrays.stream(bean.getMBeanInfo().getAttributes())
            .map(MBeanFeatureInfo::getName)
            .collect(Collectors.toList());
    return () ->
        names.stream()
            .map(
                name -> {
                  try {
                    Attribute value1 = (Attribute) bean.getAttribute(name);
                    Attribute value2 = (Attribute) bean.getAttributes(new String[] {name}).get(0);
                    assertEquals(value1, value2);
                    return (Long) value1.getValue();
                  } catch (AttributeNotFoundException | MBeanException | ReflectionException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
  }
}
