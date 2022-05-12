package org.astraea.balancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface RebalancePlanProposal {

  boolean isPlanGenerated();

  Optional<ClusterLogAllocation> rebalancePlan();

  List<String> info();

  List<String> warnings();

  static Build builder() {
    return new Build();
  }

  class Build {
    ClusterLogAllocation rebalancePlan = null;
    List<String> info = new ArrayList<>();
    List<String> warnings = new ArrayList<>();

    public Build noRebalancePlan() {
      this.rebalancePlan = null;
      return this;
    }

    public Build withRebalancePlan(ClusterLogAllocation clusterLogAllocation) {
      this.rebalancePlan = Objects.requireNonNull(clusterLogAllocation);
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

        @Override
        public boolean isPlanGenerated() {
          return rebalancePlan().isPresent();
        }

        @Override
        public Optional<ClusterLogAllocation> rebalancePlan() {
          return Optional.ofNullable(rebalancePlan);
        }

        @Override
        public List<String> info() {
          return List.copyOf(info);
        }

        @Override
        public List<String> warnings() {
          return List.copyOf(warnings);
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
