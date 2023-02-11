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
package org.astraea.app.publisher;

import org.astraea.common.metrics.BeanQuery;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JMXFetcherTest {
  Service service = Service.builder().numberOfWorkers(0).build();

  @Test
  void testFetch() {
    try (var jmxFetcher =
        JMXFetcher.create(service.jmxServiceURL().getHost(), service.jmxServiceURL().getPort())) {
      Assertions.assertNotEquals(0, jmxFetcher.fetch().size());
    }
  }

  @Test
  void testClearQuery() {
    try (var jmxFetcher =
        JMXFetcher.create(service.jmxServiceURL().getHost(), service.jmxServiceURL().getPort())) {
      jmxFetcher.clearQuery();
      Assertions.assertEquals(0, jmxFetcher.fetch().size());
    }
  }

  @Test
  void testAddQuery() {
    try (var jmxFetcher =
        JMXFetcher.create(service.jmxServiceURL().getHost(), service.jmxServiceURL().getPort())) {
      int allBeans = jmxFetcher.fetch().size();
      Assertions.assertNotEquals(0, allBeans);
      jmxFetcher.clearQuery();
      jmxFetcher.addQuery(
          BeanQuery.builder().domainName("java.lang").usePropertyListPattern().build());
      System.out.println(jmxFetcher.fetch());
      int oneQuery = jmxFetcher.fetch().size();
      Assertions.assertTrue(allBeans > oneQuery);
    }
  }
}
