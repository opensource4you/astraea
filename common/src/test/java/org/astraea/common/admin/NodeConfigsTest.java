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

import org.astraea.common.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeConfigsTest {

  @Test
  void testDynamicalConfigs() {
    Assertions.assertNotEquals(0, NodeConfigs.DYNAMICAL_CONFIGS.size());
    Assertions.assertTrue(
        NodeConfigs.DYNAMICAL_CONFIGS.contains(NodeConfigs.BACKGROUND_THREADS_CONFIG));
    Assertions.assertTrue(
        NodeConfigs.DYNAMICAL_CONFIGS.contains(NodeConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG));
    Assertions.assertTrue(
        NodeConfigs.DYNAMICAL_CONFIGS.contains(NodeConfigs.NUM_IO_THREADS_CONFIG));
    Assertions.assertTrue(
        NodeConfigs.DYNAMICAL_CONFIGS.contains(NodeConfigs.NUM_REPLICA_FETCHERS_CONFIG));
  }

  @Test
  void testDuplicate() {
    Assertions.assertEquals(
        NodeConfigs.DYNAMICAL_CONFIGS.size(),
        Utils.constants(NodeConfigs.class, name -> name.endsWith("CONFIG"), String.class).size());
  }
}
