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

class MoveCostBuilder {
  private long totalCost;
  private String unit;
  private Map<Integer, Long> changes;

  public MoveCostBuilder totalCost(long totalCost) {
    this.totalCost = totalCost;
    return this;
  }

  public MoveCostBuilder unit(String unit) {
    this.unit = unit;
    return this;
  }

  public MoveCostBuilder change(Map<Integer, Long> changes) {
    this.changes = changes;
    return this;
  }

  public MoveCost build() {
    return new MoveCost() {
      @Override
      public long totalCost() {
        return totalCost;
      }

      @Override
      public String unit() {
        return unit;
      }

      @Override
      public Map<Integer, Long> changes() {
        return changes;
      }
    };
  }
}
