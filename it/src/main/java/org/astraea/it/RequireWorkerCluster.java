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

import java.net.URL;
import java.util.Set;
import javax.management.remote.JMXServiceURL;
import org.junit.jupiter.api.AfterAll;

/**
 * This class offers a way to have 3 node embedded kafka worker. It is useful to test code which is
 * depended on true cluster.
 */
public abstract class RequireWorkerCluster {

  private static final Service SERVICE =
      Service.builder().numberOfBrokers(3).numberOfWorkers(3).build();

  protected static String bootstrapServers() {
    return SERVICE.bootstrapServers();
  }

  protected static Set<URL> workerUrls() {
    return SERVICE.workerUrls();
  }

  /**
   * @return url of any worker
   */
  protected static URL workerUrl() {
    return SERVICE.workerUrl();
  }

  protected static JMXServiceURL jmxServiceURL() {
    return SERVICE.jmxServiceURL();
  }

  @AfterAll
  static void shutdownClusters() {
    SERVICE.close();
  }
}
