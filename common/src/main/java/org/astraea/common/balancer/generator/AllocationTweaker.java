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
package org.astraea.common.balancer.generator;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.astraea.common.balancer.log.ClusterLogAllocation;

@FunctionalInterface
public interface AllocationTweaker {

  static AllocationTweaker random(int numberOfShuffle) {
    return new ShuffleTweaker(() -> numberOfShuffle);
  }

  /**
   * Given a {@link ClusterLogAllocation}, tweak it by certain implementation specific logic.
   *
   * <p>In a nutshell. This function takes a {@link ClusterLogAllocation} and return another
   * modified version of the given {@link ClusterLogAllocation}. The caller can use this method for
   * browsing the space of possible {@link ClusterLogAllocation}.
   *
   * <p>If the implementation find no alternative feasible {@link ClusterLogAllocation}. Then an
   * empty {@link Stream} will be returned. We don't encourage the implementation to return the
   * original {@link ClusterLogAllocation} as part of the Stream result. Since there is no tweaking
   * occurred.
   *
   * @param brokerFolders key is the broker id, and the value is the folder used to keep data
   * @param baseAllocation the cluster log allocation as the based of proposal generation.
   * @return a {@link Stream} of possible tweaked {@link ClusterLogAllocation}.
   */
  Stream<ClusterLogAllocation> generate(
      Map<Integer, Set<String>> brokerFolders, ClusterLogAllocation baseAllocation);
}
