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
package org.astraea.app.cost;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.Replica;

public class MoveCostUtils {
  static class MigrateInfo {
    long totalNum;
    Map<Integer, Long> changes;

    MigrateInfo(long totalNum, Map<Integer, Long> changes) {
      this.totalNum = totalNum;
      this.changes = changes;
    }
  }

  static MoveCost moveCost(
      String name,
      String unit,
      ClusterInfo<Replica> before,
      ClusterInfo<Replica> after,
      BiFunction<Collection<Replica>, Collection<Replica>, MoveCostUtils.MigrateInfo>
          getMigrateInfo) {
    var removedReplicas = ClusterInfo.diff(before, after);
    var addedReplicas = ClusterInfo.diff(after, before);
    var migrateInfo = getMigrateInfo.apply(removedReplicas, addedReplicas);
    return new MoveCost() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public long totalCost() {
        return migrateInfo.totalNum;
      }

      @Override
      public String unit() {
        return unit;
      }

      @Override
      public Map<Integer, Long> changes() {
        return migrateInfo.changes;
      }
    };
  }
}
