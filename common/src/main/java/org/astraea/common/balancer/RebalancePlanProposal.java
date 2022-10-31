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
package org.astraea.common.balancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.astraea.common.balancer.log.ClusterLogAllocation;

public interface RebalancePlanProposal {

  /**
   * @return the index of this proposal in stream
   */
  int index();

  ClusterLogAllocation rebalancePlan();

  List<String> info();

  List<String> warnings();

  static Build builder() {
    return new Build();
  }

  class Build {

    int index = 0;

    ClusterLogAllocation allocation = null;
    List<String> info = new ArrayList<>();
    List<String> warnings = new ArrayList<>();

    public synchronized Build index(int index) {
      this.index = index;
      return this;
    }

    public synchronized Build clusterLogAllocation(ClusterLogAllocation clusterLogAllocation) {
      this.allocation = Objects.requireNonNull(clusterLogAllocation);
      return this;
    }

    public synchronized Build addWarning(String warning) {
      if (warnings == null) this.warnings = new ArrayList<>();
      this.warnings.add(warning);
      return this;
    }

    public synchronized Build addInfo(String info) {
      if (info == null) this.info = new ArrayList<>();
      this.info.add(info);
      return this;
    }

    public synchronized RebalancePlanProposal build() {

      try {
        return new RebalancePlanProposal() {
          private final int index = Build.this.index;
          private final ClusterLogAllocation allocation =
              Objects.requireNonNull(
                  Build.this.allocation, () -> "No log allocation specified for this proposal");
          private final List<String> info = Collections.unmodifiableList(Build.this.info);
          private final List<String> warnings = Collections.unmodifiableList(Build.this.warnings);

          @Override
          public int index() {
            return index;
          }

          @Override
          public ClusterLogAllocation rebalancePlan() {
            return allocation;
          }

          @Override
          public List<String> info() {
            return info;
          }

          @Override
          public List<String> warnings() {
            return warnings;
          }

          @Override
          public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("[RebalancePlanProposal]").append(System.lineSeparator());

            sb.append("  Info:").append(System.lineSeparator());
            if (info().isEmpty()) sb.append(String.format("    no information%n"));
            else info().forEach(infoString -> sb.append(String.format("    * %s%n", infoString)));
            if (!warnings().isEmpty()) {
              sb.append("  Warning:").append(System.lineSeparator());
              warnings()
                  .forEach(warningString -> sb.append(String.format("    * %s%n", warningString)));
            }

            return sb.toString();
          }
        };
      } finally {
        index = 0;
        allocation = null;
        info = null;
        warnings = null;
      }
    }
  }
}
