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
package org.astraea.common.consumer.assignor;

import java.util.List;
import java.util.Map;
import org.astraea.common.admin.NodeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AbstractConsumerPartitionAssignorTest {
  @Test
  void testUnregisterId() {
    var assignor = new RandomAssignor();
    assignor.configure(Map.of("broker.1000.jmx.port", "8000", "broker.1001.jmx.port", "8100"));
    var nodes =
        List.of(NodeInfo.of(1000, "192.168.103.1", 8000), NodeInfo.of(1001, "192.168.103.2", 8100));
    var unregister = assignor.checkUnregister(nodes);
    Assertions.assertEquals(2, unregister.size());
    Assertions.assertEquals("192.168.103.1", unregister.get(1000));
    Assertions.assertEquals("192.168.103.2", unregister.get(1001));

    assignor.registerLocalJMX(Map.of(1000, "192.168.103.1"));
    var unregister2 = assignor.checkUnregister(nodes);
    Assertions.assertEquals(1, unregister2.size());
    Assertions.assertEquals("192.168.103.2", unregister2.get(1001));
  }
}
