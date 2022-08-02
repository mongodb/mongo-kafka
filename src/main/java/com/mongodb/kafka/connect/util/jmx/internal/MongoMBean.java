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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

public class MongoMBean implements DynamicMBean {
  protected static final long[] MS_LIMITS = new long[] {1, 10, 100, 1000, 10000};

  private String mBeanName;
  private final List<Metric> metrics = new ArrayList<>();
  private final Map<String, MetricValue> metricsMap = new HashMap<>();

  protected MongoMBean(final String mBeanName) {
    this.mBeanName = mBeanName;
  }

  public String getName() {
    return mBeanName;
  }

  protected TotalMetric registerTotal(final String name) {
    return register(new TotalMetric(name));
  }

  protected LatestMetric registerLatest(final String name) {
    return register(new LatestMetric(name));
  }

  protected AdditiveMetric registerMs(final String name) {
    return register(new AdditiveMetric(name, "ms", MS_LIMITS));
  }

  protected <T extends Metric> T register(final T m) {
    metrics.add(m);
    m.emit(value -> metricsMap.put(value.getName(), value));
    return m;
  }

  public void emit(final Consumer<MetricValue> consumer) {
    this.metrics.forEach(x -> x.emit(consumer));
  }

  public String toJSON() {
    StringBuilder sb = new StringBuilder("{");
    emit(
        (v) -> {
          if (sb.length() > 1) {
            sb.append(", ");
          }
          sb.append("\"").append(v.getName()).append("\": ");
          sb.append(v.get());
        });
    sb.append("}");
    return sb.toString();
  }

  @Override
  public Object getAttribute(final String name) throws AttributeNotFoundException {
    if (metricsMap.containsKey(name)) {
      return metricsMap.get(name).get();
    } else {
      throw new AttributeNotFoundException("getAttribute failed: value not found for: " + name);
    }
  }

  @Override
  public AttributeList getAttributes(final String[] attributes) {
    AttributeList list = new AttributeList();
    for (String name : attributes) {
      if (metricsMap.containsKey(name)) {
        list.add(metricsMap.get(name).get());
      }
    }
    return list;
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    List<MBeanAttributeInfo> attrs = new ArrayList<>();
    for (Metric metric : this.metrics) {
      metric.emit(
          (value) ->
              attrs.add(
                  new MBeanAttributeInfo(
                      value.getName(), long.class.getName(), null, true, false, false)));
    }
    return new MBeanInfo(
        this.getClass().getName(),
        null,
        attrs.toArray(new MBeanAttributeInfo[0]),
        null,
        null,
        null);
  }

  @Override
  public Object invoke(final String actionName, final Object[] params, final String[] signature) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAttribute(final Attribute attribute) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AttributeList setAttributes(final AttributeList attributes) {
    throw new UnsupportedOperationException();
  }

  public void register() {
    mBeanName = MBeanServerUtils.registerMBean(this, mBeanName);
  }

  public void unregister() {
    MBeanServerUtils.unregisterMBean(mBeanName);
  }
}
