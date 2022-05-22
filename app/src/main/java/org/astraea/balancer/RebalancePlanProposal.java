package org.astraea.balancer;

import java.util.ArrayList;
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
    List<String> info = new ArrayList<>();
    List<String> warnings = new ArrayList<>();

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

    public RebalancePlanProposal build() {
      return new RebalancePlanProposal() {

        private final List<String> infoList = List.copyOf(info);
        private final List<String> warningList = List.copyOf(warnings);
        private final ClusterLogAllocation allocation =
                Build.this.allocation == null ? null :
                ClusterLogAllocation.of(Build.this.allocation.allocation());

        @Override
        public Optional<ClusterLogAllocation> rebalancePlan() {
          return Optional.ofNullable(allocation);
        }

        @Override
        public List<String> info() {
          return infoList;
        }

        @Override
        public List<String> warnings() {
          return warningList;
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
