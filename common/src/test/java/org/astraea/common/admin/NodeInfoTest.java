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
package org.astraea.common.admin;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class NodeInfoTest {

  static Node node() {
    return new Node(10, "host", 100);
  }

  @Test
  void testAllGetters() {
    var kafkaNode = node();
    var node = NodeInfo.of(kafkaNode);

    Assertions.assertEquals(kafkaNode.host(), node.host());
    Assertions.assertEquals(kafkaNode.id(), node.id());
    Assertions.assertEquals(kafkaNode.port(), node.port());
  }

  @DisplayName("Broker can interact with normal NodeInfo properly")
  @ParameterizedTest
  @CsvSource(
      value = {
        "  1, host1, 1000",
        " 20, host2, 2000",
        "300, host3, 3000",
      })
  void testFakeBrokerInteraction(int id, String host, int port) {
    var node = NodeInfo.of(id, host, port);
    var broker = Broker.of(false, new Node(id, host, port), Map.of(), Map.of(), List.of());
    var nodeOther = NodeInfo.of(id + 1, host, port);

    Assertions.assertEquals(node.hashCode(), broker.hashCode());
    Assertions.assertEquals(node, broker);
    Assertions.assertNotEquals(nodeOther.hashCode(), broker.hashCode());
    Assertions.assertNotEquals(nodeOther, broker);
  }
}
