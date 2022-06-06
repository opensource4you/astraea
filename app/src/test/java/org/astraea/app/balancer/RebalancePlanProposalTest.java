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
package org.astraea.app.balancer;

import java.util.stream.Collectors;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.cost.ClusterInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RebalancePlanProposalTest {

  @Test
  void testBuild() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(10, 10, 10, 10);
    final var thisAllocation = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var build =
        RebalancePlanProposal.builder()
            .withRebalancePlan(thisAllocation)
            .addInfo("Info0")
            .addInfo("Info1")
            .addInfo("Info2")
            .addWarning("Warning0")
            .addWarning("Warning1")
            .addWarning("Warning2")
            .build();

    Assertions.assertTrue(build.rebalancePlan().isPresent());
    final var thatAllocation = build.rebalancePlan().orElseThrow();
    final var thisTps =
        thisAllocation.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
    final var thatTps =
        thatAllocation.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
    Assertions.assertEquals(thisTps, thatTps);
    thisTps.forEach(
        tp ->
            Assertions.assertEquals(
                thisAllocation.logPlacements(tp), thatAllocation.logPlacements(tp)));
    Assertions.assertEquals("Info0", build.info().get(0));
    Assertions.assertEquals("Info1", build.info().get(1));
    Assertions.assertEquals("Info2", build.info().get(2));
    Assertions.assertEquals("Warning0", build.warnings().get(0));
    Assertions.assertEquals("Warning1", build.warnings().get(1));
    Assertions.assertEquals("Warning2", build.warnings().get(2));
  }

  @Test
  void testNoBuildTwice() {
    // A builder should only build once. If a builder can build multiple times then it will have to
    // do much copy work once a new build is requested. This will harm performance.
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(10, 10, 10, 10);
    final var logAllocation = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var build = RebalancePlanProposal.builder().withRebalancePlan(logAllocation);

    Assertions.assertDoesNotThrow(build::build);
    Assertions.assertThrows(IllegalStateException.class, build::build);
  }

  @Test
  void testNoModifyAfterBuild() {
    final var fakeClusterInfo = ClusterInfoProvider.fakeClusterInfo(10, 10, 10, 10);
    final var logAllocation = LayeredClusterLogAllocation.of(fakeClusterInfo);
    final var build = RebalancePlanProposal.builder().withRebalancePlan(logAllocation);

    RebalancePlanProposal proposal = build.build();
    Assertions.assertThrows(
        IllegalStateException.class, () -> build.addInfo("modify after built."));
    Assertions.assertFalse(proposal.info().contains("modify after built."));
  }
}
