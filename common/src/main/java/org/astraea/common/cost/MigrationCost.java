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

import static org.astraea.common.admin.ClusterInfo.recordSizeToFetch;
import static org.astraea.common.admin.ClusterInfo.recordSizeToSync;
import static org.astraea.common.admin.ClusterInfo.replicaLeaderChanged;
import static org.astraea.common.admin.ClusterInfo.replicaNumChanged;

import java.util.List;
import java.util.Map;
import org.astraea.common.admin.ClusterInfo;

public class MigrationCost {

  public final String name;

  public final Map<Integer, Long> brokerCosts;

  public static List<MigrationCost> migrationCosts(ClusterInfo before, ClusterInfo after) {
    var migrateInBytes = recordSizeToSync(before, after);
    var migrateOutBytes = recordSizeToFetch(before, after);
    var migrateReplicaNum = replicaNumChanged(before, after);
    var migrateReplicaLeader = replicaLeaderChanged(before, after);
    return List.of(
        new MigrationCost(RecordSizeCost.TO_SYNC_BYTES, migrateInBytes),
        new MigrationCost(RecordSizeCost.TO_FETCH_BYTES, migrateOutBytes),
        new MigrationCost(ReplicaNumberCost.CHANGED_REPLICAS, migrateReplicaNum),
        new MigrationCost(ReplicaLeaderCost.CHANGED_LEADERS, migrateReplicaLeader));
  }

  public MigrationCost(String name, Map<Integer, Long> brokerCosts) {
    this.name = name;
    this.brokerCosts = brokerCosts;
  }
}
