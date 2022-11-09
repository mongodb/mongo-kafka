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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;

public class CombinedMongoMBean implements DynamicMBean {
  private String mBeanName;
  private final MongoMBean a;
  private final Map<String, MetricValue> metricsMap = new LinkedHashMap<>();

  public <T extends MongoMBean> CombinedMongoMBean(final String mBeanName, final T a, final T b) {
    this.mBeanName = mBeanName;
    this.a = a;
    // combine values:
    Map<String, MetricValue> metricsMap1 = new HashMap<>();
    a.emit(value1 -> metricsMap1.put(value1.getName(), value1));
    b.emit(
        value2 -> {
          MetricValue value1 = metricsMap1.get(value2.getName());
          metricsMap.put(value2.getName(), value2.combine(value1));
        });
  }

  @Override
  public Object getAttribute(final String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    if (metricsMap.containsKey(attribute)) {
      return new Attribute(attribute, metricsMap.get(attribute).get());
    } else {
      throw new AttributeNotFoundException(
          "getAttribute failed: value not found for: " + attribute);
    }
  }

  @Override
  public AttributeList getAttributes(final String[] attributes) {
    AttributeList list = new AttributeList();
    for (String name : attributes) {
      if (metricsMap.containsKey(name)) {
        list.add(new Attribute(name, metricsMap.get(name).get()));
      }
    }
    return list;
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    // info is the same for both MBeans provided in constructor
    return a.getMBeanInfo();
  }

  @Override
  public Object invoke(final String actionName, final Object[] params, final String[] signature)
      throws MBeanException, ReflectionException {
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

  public String getName() {
    return mBeanName;
  }
}
