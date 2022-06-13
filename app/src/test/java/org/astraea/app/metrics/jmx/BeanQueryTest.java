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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

class BeanQueryTest {

  @Test
  void domainName() {
    BeanQuery beanQuery =
        BeanQuery.builder("java.lang")
            .property("type", "MemoryManager")
            .property("name", "CodeCacheManager")
            .build();

    assertEquals("java.lang", beanQuery.domainName());
  }

  @Test
  void propertyView() {
    BeanQuery beanQuery =
        BeanQuery.builder("java.lang")
            .property("type", "MemoryManager")
            .property("name", "CodeCacheManager")
            .build();

    assertEquals(
        Map.of("type", "MemoryManager", "name", "CodeCacheManager"), beanQuery.properties());
  }

  @Test
  void objectName() throws MalformedObjectNameException {
    BeanQuery beanQuery =
        BeanQuery.builder("java.lang")
            .property("type", "MemoryManager")
            .property("name", "CodeCacheManager")
            .build();

    assertEquals(
        ObjectName.getInstance("java.lang:type=MemoryManager,name=CodeCacheManager"),
        beanQuery.objectName());
  }

  @Test
  void whereProperty() {
    BeanQuery.BeanQueryBuilder beanQueryBuilder =
        BeanQuery.builder("java.lang")
            .property("type", "MemoryManager")
            .property("name", "CodeCacheManager");

    BeanQuery beanQuery0 = beanQueryBuilder.build();
    BeanQuery beanQuery1 = beanQueryBuilder.property("hello", "world").build();

    assertTrue(beanQuery1.properties().containsKey("hello"));
    assertTrue(beanQuery1.properties().containsValue("world"));
    assertFalse(beanQuery0.properties().containsKey("hello"));
    assertFalse(beanQuery0.properties().containsValue("world"));
  }

  @Test
  void of() throws MalformedObjectNameException {
    // ObjectName version
    BeanQuery beanQueryFromObjectName =
        BeanQuery.fromObjectName(ObjectName.getInstance("java.lang:type=Memory"));
    assertEquals("java.lang", beanQueryFromObjectName.domainName());
    assertEquals(Map.of("type", "Memory"), beanQueryFromObjectName.properties());

    // map version
    BeanQuery beanQueryFromMap = BeanQuery.builder("java.lang", Map.of("type", "Memory")).build();
    assertEquals("java.lang", beanQueryFromMap.domainName());
    assertEquals(Map.of("type", "Memory"), beanQueryFromMap.properties());

    // all under specific domain
    BeanQuery beanQueryForDomain = BeanQuery.all("java.lang");
    assertEquals(0, beanQueryForDomain.objectName().getKeyPropertyList().size());
    assertEquals("java.lang", beanQueryForDomain.objectName().getDomain());

    // all in JMX
    BeanQuery beanQueryForAllDomain = BeanQuery.all();
    assertEquals(0, beanQueryForAllDomain.objectName().getKeyPropertyList().size());
    assertEquals("*", beanQueryForAllDomain.objectName().getDomain());

    // usePropertyListPattern
    BeanQuery withPattern =
        BeanQuery.builder("java.lang")
            .property("type", "MemoryManager")
            .usePropertyListPattern()
            .build();
    assertTrue(withPattern.objectName().isPropertyListPattern());

    // invalid query: query whole domain name without specify "property list pattern"
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          BeanQuery.builder("java.lang").build();
        });
  }
}
