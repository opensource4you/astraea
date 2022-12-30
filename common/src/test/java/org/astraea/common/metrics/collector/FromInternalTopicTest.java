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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.metrics.BeanObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FromInternalTopicTest {
  @Test
  void testClusterBean() {
    try (var collector = new FromInternalTopic("192.168.103.26:9092")) {
      for (int i = 0; i < 10; ++i) {
        Thread.sleep(5000);
        getLogEndOffset(collector).forEach(System.out::println);
        System.out.println("=====");
      }
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }
  }

  @Test
  void testStringToBean() {
    var bean =
        new BeanObject("domain", Map.of("name", "name1", "type", "type1"), Map.of("value", "v1"));
    Assertions.assertEquals(bean, FromInternalTopic.stringToBean(bean.toString()).beanObject());
  }

  private List<String> getLogEndOffset(MetricCollector collector) {
    return collector.clusterBean().all().values().stream()
        .flatMap(Collection::stream)
        .filter(bean -> bean.beanObject().domainName().equals("kafka.log"))
        .filter(bean -> bean.beanObject().properties().getOrDefault("name", "").equals("Size"))
        .filter(bean -> bean.beanObject().properties().getOrDefault("topic", "").equals("simple"))
        .map(hbo -> hbo.beanObject().toString())
        .collect(Collectors.toUnmodifiableList());
  }
}
