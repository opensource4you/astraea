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
package org.astraea.common.cost;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;

final class CostUtils {

  private CostUtils() {}

  static boolean changedRecordSizeOverflow(
      ClusterInfo before, ClusterInfo after, Predicate<Replica> predicate, long limit) {
    var totalRemovedSize = 0L;
    var totalAddedSize = 0L;
    for (var id :
        Stream.concat(before.brokers().stream(), after.brokers().stream())
            .map(Broker::id)
            .parallel()
            .collect(Collectors.toSet())) {
      var removed =
          (int)
              before
                  .replicaStream(id)
                  .filter(predicate)
                  .filter(r -> !after.replicas(r.topicPartition()).contains(r))
                  .mapToLong(Replica::size)
                  .sum();
      var added =
          (int)
              after
                  .replicaStream(id)
                  .filter(predicate)
                  .filter(r -> !before.replicas(r.topicPartition()).contains(r))
                  .mapToLong(Replica::size)
                  .sum();
      totalRemovedSize = totalRemovedSize + removed;
      totalAddedSize = totalAddedSize + added;
      // if migrate cost overflow, leave early and return true
      if (totalRemovedSize > limit || totalAddedSize > limit) return true;
    }
    return Math.max(totalRemovedSize, totalAddedSize) > limit;
  }
}
