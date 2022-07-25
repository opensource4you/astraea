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
package org.astraea.app.metrics;

import java.time.Duration;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.jmx.MBeanClient;
import org.astraea.app.service.RequireSingleBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class KafkaLogMetricsTest extends RequireSingleBrokerCluster {

  @ParameterizedTest
  @EnumSource(KafkaLogMetrics.Log.class)
  void testMetrics(KafkaLogMetrics.Log log) {
    var topicName = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topicName).numberOfPartitions(2).create();
      Utils.sleep(Duration.ofSeconds(2));
      var beans =
          log.fetch(MBeanClient.local()).stream()
              .filter(m -> m.topic().equals(topicName))
              .collect(Collectors.toUnmodifiableList());
      Assertions.assertEquals(2, beans.size());
    }
  }

  @ParameterizedTest
  @EnumSource(KafkaLogMetrics.Log.class)
  void testValue(KafkaLogMetrics.Log log) {
    log.fetch(MBeanClient.local()).forEach(m -> Assertions.assertTrue(m.value() >= 0));
  }
}
