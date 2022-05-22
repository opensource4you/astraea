package org.astraea.balancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    private volatile boolean built = false;

    public Build noRebalancePlan() {
      this.allocation = null;
      return this;
    }

    public Build withRebalancePlan(ClusterLogAllocation clusterLogAllocation) {
      this.allocation = Objects.requireNonNull(clusterLogAllocation);
      return this;
    }

    public Build addWarning(String warning) {
      this.warnings.add(warning);
      return this;
    }

    public Build addInfo(String info) {
      this.info.add(info);
      return this;
    }

    public synchronized RebalancePlanProposal build() {
      if (built) throw new IllegalStateException("This builder already built.");
      else built = true;
      return new RebalancePlanProposal() {

        @Override
        public Optional<ClusterLogAllocation> rebalancePlan() {
          return Optional.ofNullable(allocation);
        }

        @Override
        public List<String> info() {
          // use Collections.unmodifiableList instead of List.copyOf to avoid excessive memory
          // footprint
          return Collections.unmodifiableList(info);
        }

        @Override
        public List<String> warnings() {
          // use Collections.unmodifiableList instead of List.copyOf to avoid excessive memory
          // footprint
          return Collections.unmodifiableList(warnings);
        }

        @Override
        public String toString() {
          StringBuilder sb = new StringBuilder();

          sb.append("[RebalancePlanProposal]").append(System.lineSeparator());

          sb.append("  Info:").append(System.lineSeparator());
          if (info.isEmpty()) sb.append(String.format("    no information%n"));
          else info.forEach(infoString -> sb.append(String.format("    * %s%n", infoString)));
          if (!warnings.isEmpty()) {
            sb.append("  Warning:").append(System.lineSeparator());
            warnings.forEach(
                warningString -> sb.append(String.format("    * %s%n", warningString)));
          }

          return sb.toString();
        }
      };
    }
  }
}
