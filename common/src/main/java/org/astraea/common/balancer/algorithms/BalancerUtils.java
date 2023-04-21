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
package org.astraea.common.balancer.algorithms;

import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.astraea.common.admin.Replica;

public final class BalancerUtils {

  private BalancerUtils() {}

  public static boolean eligiblePartition(Collection<Replica> replicas) {
    return Stream.<Predicate<Collection<Replica>>>of(
            // only one replica and it is offline
            r -> r.size() == 1 && r.stream().findFirst().orElseThrow().isOffline(),
            // no leader
            r -> r.stream().noneMatch(Replica::isLeader))
        .noneMatch(p -> p.test(replicas));
  }
}
