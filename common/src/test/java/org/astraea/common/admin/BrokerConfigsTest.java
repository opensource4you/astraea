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

public class BrokerConfigsTest {

  @Test
  void testDynamicalConfigs() {
    Assertions.assertNotEquals(0, BrokerConfigs.DYNAMICAL_CONFIGS.size());
    Assertions.assertTrue(
        BrokerConfigs.DYNAMICAL_CONFIGS.contains(BrokerConfigs.ADVERTISED_LISTENERS_CONFIG));
    Assertions.assertTrue(
        BrokerConfigs.DYNAMICAL_CONFIGS.contains(BrokerConfigs.BACKGROUND_THREADS_CONFIG));
    Assertions.assertTrue(
        BrokerConfigs.DYNAMICAL_CONFIGS.contains(
            BrokerConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG));
    Assertions.assertTrue(
        BrokerConfigs.DYNAMICAL_CONFIGS.contains(BrokerConfigs.NUM_IO_THREADS_CONFIG));
    Assertions.assertTrue(
        BrokerConfigs.DYNAMICAL_CONFIGS.contains(BrokerConfigs.NUM_REPLICA_FETCHERS_CONFIG));
  }

  @Test
  void testDuplicate() {
    Assertions.assertEquals(
        BrokerConfigs.DYNAMICAL_CONFIGS.size(),
        Utils.constants(BrokerConfigs.class, name -> name.endsWith("CONFIG")).size());
  }
}
