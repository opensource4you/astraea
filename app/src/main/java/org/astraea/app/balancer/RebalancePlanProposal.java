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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.astraea.app.balancer.log.ClusterLogAllocation;

public interface RebalancePlanProposal {

  Optional<ClusterLogAllocation> rebalancePlan();

  List<String> info();

  List<String> warnings();

  static Build builder() {
    return new Build();
  }

  class Build {
    ClusterLogAllocation allocation = null;
    List<String> info = Collections.synchronizedList(new ArrayList<>());
    List<String> warnings = Collections.synchronizedList(new ArrayList<>());

    // guard by this
    private boolean built = false;

    public synchronized Build noRebalancePlan() {
      ensureNotBuiltYet();
      this.allocation = null;
      return this;
    }

    public synchronized Build withRebalancePlan(ClusterLogAllocation clusterLogAllocation) {
      ensureNotBuiltYet();
      this.allocation = Objects.requireNonNull(clusterLogAllocation);
      return this;
    }

    public synchronized Build addWarning(String warning) {
      ensureNotBuiltYet();
      this.warnings.add(warning);
      return this;
    }

    public synchronized Build addInfo(String info) {
      ensureNotBuiltYet();
      this.info.add(info);
      return this;
    }

    private synchronized void ensureNotBuiltYet() {
      if (built) throw new IllegalStateException("This builder already built.");
    }

    public synchronized RebalancePlanProposal build() {
      final var allocationRef = allocation;
      final var infoRef = info;
      final var warningRef = warnings;

      ensureNotBuiltYet();

      built = true;
      allocation = null;
      info = null;
      warnings = null;

      return new RebalancePlanProposal() {

        @Override
        public Optional<ClusterLogAllocation> rebalancePlan() {
          return Optional.ofNullable(allocationRef);
        }

        @Override
        public List<String> info() {
          // use Collections.unmodifiableList instead of List.copyOf to avoid excessive memory
          // footprint
          return Collections.unmodifiableList(infoRef);
        }

        @Override
        public List<String> warnings() {
          // use Collections.unmodifiableList instead of List.copyOf to avoid excessive memory
          // footprint
          return Collections.unmodifiableList(warningRef);
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
    }
  }
}
