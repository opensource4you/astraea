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
package org.astraea.common.metrics.collector;

import java.util.Map;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FromInternalTopicTest {
  @Test
  void testStringToBean() {
    var bean =
        new BeanObject("domain", Map.of("name", "name1", "type", "type1"), Map.of("value", "v1"));
    Assertions.assertEquals(bean, FromInternalTopic.stringToBean(bean.toString()));
  }

  @Test
  void testMetricStoreQuery() {
    var metricStore = new FromInternalTopic.MetricStore();
    var bean1 =
        new BeanObject("kafka", Map.of("name", "name1", "type", "type1"), Map.of("value", "v1"));
    var bean2 = new BeanObject("kafka", Map.of("name", "no", "type", "no"), Map.of("value", "v2"));
    metricStore.put(
        new FromInternalTopic.BeanProperties(bean1.domainName(), bean1.properties()), bean1);
    metricStore.put(
        new FromInternalTopic.BeanProperties(bean2.domainName(), bean2.properties()), bean2);

    Assertions.assertEquals(
        2, metricStore.queryBeans(BeanQuery.builder().property("name", "*").build()).size());
    Assertions.assertEquals(
        bean1, metricStore.queryBean(BeanQuery.builder().property("name", "name1").build()));
    Assertions.assertEquals(
        bean2, metricStore.queryBean(BeanQuery.builder().property("name", "no").build()));
  }
}
