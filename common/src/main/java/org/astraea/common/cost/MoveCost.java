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

/** Return type of cost function, `HasMoveCost`. It returns the score of migrate plan. */
public interface MoveCost {

  /** @return the function name of MoveCost */
  String name();

  /** @return cost of migrate plan */
  long totalCost();

  /** @return unit of cost */
  String unit();

  /** @return Changes per broker, negative if brokers moved out, positive if brokers moved in */
  Map<Integer, Long> changes();

  static Build builder() {
    return new Build();
  }

  class Build {
    private String name = "unknown";
    private long totalCost;
    private String unit = "unknown";
    private Map<Integer, Long> changes = Map.of();

    private Build() {}

    public Build name(String name) {
      this.name = name;
      return this;
    }

    public Build totalCost(long totalCost) {
      this.totalCost = totalCost;
      return this;
    }

    public Build unit(String unit) {
      this.unit = unit;
      return this;
    }

    public Build change(Map<Integer, Long> changes) {
      this.changes = changes;
      return this;
    }

    public MoveCost build() {
      return new MoveCost() {
        @Override
        public String name() {
          return name;
        }

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
}
