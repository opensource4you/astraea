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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
import org.junit.jupiter.api.Test;

class RequireWorkerClusterTest extends RequireWorkerCluster {

  @Test
  void testProperties() {
    assertTrue(workerUrls().size() > 0);
    assertNotNull(bootstrapServers());
  }

  @Test
  void testConnection() throws Exception {
    for (String x : workerUrls()) {
      var jsonTree = new ObjectMapper().readTree(new URL(x));
      assertNotNull(jsonTree.get("version"));
      assertNotNull(jsonTree.get("kafka_cluster_id"));
    }
  }
}
