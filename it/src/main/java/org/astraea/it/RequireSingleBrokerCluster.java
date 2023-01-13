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
package org.astraea.it;

import java.util.Map;
import java.util.Set;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.AfterAll;

/**
 * This class offers a way to have embedded kafka cluster. It is useful to test code which is
 * depended on true cluster.
 */
public abstract class RequireSingleBrokerCluster {

  private static final Service SERVICE = Service.builder().numberOfBrokers(1).build();

  protected static String bootstrapServers() {
    return SERVICE.bootstrapServers();
  }

  protected static Map<Integer, Set<String>> dataFolders() {
    return SERVICE.dataFolders();
  }

  protected static int brokerId() {
    return dataFolders().keySet().iterator().next();
  }

  protected static JMXServiceURL jmxServiceURL() {
    return SERVICE.jmxServiceURL();
  }

  @AfterAll
  static void shutdownClusters() {
    SERVICE.close();
  }
}
