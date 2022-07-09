/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.metrics.jmx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class BeanObjectTest {

  BeanObject createBeanObject() {
    return new BeanObject(
        "java.lang", Map.of("type", "Memory"), Map.of("HeapMemoryUsage", "content"));
  }

  @Test
  void domainName() {
    BeanObject beanObject = createBeanObject();

    String domainName = beanObject.domainName();

    assertEquals("java.lang", domainName);
  }

  @Test
  void getPropertyView() {
    BeanObject beanObject = createBeanObject();

    Map<String, String> propertyView = beanObject.properties();

    assertEquals(Map.of("type", "Memory"), propertyView);
  }

  @Test
  void getAttributeView() {
    BeanObject beanObject = createBeanObject();

    Map<String, Object> result = beanObject.attributes();

    assertEquals(Map.of("HeapMemoryUsage", "content"), result);
  }

  @Test
  void testToString() {
    BeanObject beanObject = createBeanObject();

    String s = beanObject.toString();

    assertTrue(s.contains(beanObject.domainName()));
  }
}
