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

import java.util.Map;
import org.astraea.common.DataSize;

/** Return type of cost function, `HasMoveCost`. It returns the score of migrate plan. */
public interface MoveCost {

  MoveCost EMPTY = new MoveCost() {};

  static MoveCost movedReplicaSize(Map<Integer, DataSize> value) {
    return new MoveCost() {
      @Override
      public Map<Integer, DataSize> movedReplicaSize() {
        return value;
      }
    };
  }

  static MoveCost changedReplicaCount(Map<Integer, Integer> value) {
    return new MoveCost() {
      @Override
      public Map<Integer, Integer> changedReplicaCount() {
        return value;
      }
    };
  }

  static MoveCost changedReplicaLeaderCount(Map<Integer, Integer> value) {
    return new MoveCost() {
      @Override
      public Map<Integer, Integer> changedReplicaLeaderCount() {
        return value;
      }
    };
  }

  /**
   * @return the data size of moving replicas. Noted that the "removing" replicas are excluded.
   */
  default Map<Integer, DataSize> movedReplicaSize() {
    return Map.of();
  }

  /**
   * @return broker id and changed number of replicas. changed number = (number of adding replicas -
   *     number of removing replicas)
   */
  default Map<Integer, Integer> changedReplicaCount() {
    return Map.of();
  }

  /**
   * @return broker id and changed number of leaders. changed number = (number of adding leaders -
   *     number of removing leaders)
   */
  default Map<Integer, Integer> changedReplicaLeaderCount() {
    return Map.of();
  }
}
