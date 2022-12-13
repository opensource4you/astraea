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
package org.astraea.gui.tab;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SettingNodeTest {

  @Test
  void testProp() {
    var prop = new SettingNode.Prop();

    prop.bootstrapServers = "abc";
    SettingNode.save(prop);
    assertEqual(prop, SettingNode.load().get());

    prop.brokerJmxPort = Optional.of(123);
    SettingNode.save(prop);
    assertEqual(prop, SettingNode.load().get());

    prop.workerUrl = Optional.of("acc");
    SettingNode.save(prop);
    assertEqual(prop, SettingNode.load().get());

    prop.workerJmxPort = Optional.of(1222);
    SettingNode.save(prop);
    assertEqual(prop, SettingNode.load().get());
  }

  private static void assertEqual(SettingNode.Prop lhs, SettingNode.Prop rhs) {
    Assertions.assertEquals(lhs.bootstrapServers, rhs.bootstrapServers);
    Assertions.assertEquals(lhs.brokerJmxPort, rhs.brokerJmxPort);
    Assertions.assertEquals(lhs.workerUrl, rhs.workerUrl);
    Assertions.assertEquals(lhs.workerJmxPort, rhs.workerJmxPort);
  }
}
