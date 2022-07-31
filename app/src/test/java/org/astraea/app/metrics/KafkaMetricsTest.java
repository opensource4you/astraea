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

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.admin.Admin;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.broker.LogMetrics;
import org.astraea.app.service.RequireBrokerCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class KafkaMetricsTest extends RequireBrokerCluster {

  private MBeanServer mBeanServer;
  private JMXConnectorServer jmxServer;
  private MBeanClient mBeanClient;

  @BeforeEach
  void setUp() throws IOException {
    JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi://127.0.0.1");

    mBeanServer = ManagementFactory.getPlatformMBeanServer();

    jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, mBeanServer);
    jmxServer.start();

    mBeanClient = MBeanClient.of(jmxServer.getAddress());
  }

  @AfterEach
  void tearDown() throws Exception {
    jmxServer.stop();
    mBeanServer = null;
    mBeanClient.close();
  }

  @ParameterizedTest()
  @EnumSource(value = LogMetrics.Log.class)
  void testTopicPartitionMetrics(LogMetrics.Log request) {
    try (var admin = Admin.of(bootstrapServers())) {
      // there are only 3 brokers, so 10 partitions can make each broker has some partitions
      admin.creator().topic(Utils.randomString(5)).numberOfPartitions(10).create();
    }

    // wait for topic creation
    Utils.sleep(Duration.ofSeconds(2));

    var beans = request.fetch(mBeanClient);
    assertNotEquals(0, beans.size());
  }
}
