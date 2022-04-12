package org.astraea.balancer.alpha;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface RebalancePlanProposal {

  boolean isPlanGenerated();

  Optional<ClusterLogAllocation> rebalancePlan();

  List<String> info();

  List<String> warnings();

  List<Exception> exceptions();

  static Build builder() {
    return new Build();
  }

  class Build {
    ClusterLogAllocation rebalancePlan = null;
    List<String> info = new ArrayList<>();
    List<String> warnings = new ArrayList<>();
    List<Exception> exceptions = new ArrayList<>();

    public Build noRebalancePlan() {
      this.rebalancePlan = null;
      return this;
    }

    public Build withRebalancePlan(ClusterLogAllocation plan) {
      this.rebalancePlan = Objects.requireNonNull(plan);
      return this;
    }

    public Build addWarning(List<String> warning) {
      this.warnings.addAll(warning);
      return this;
    }

    public Build addInfo(List<String> info) {
      this.info.addAll(info);
      return this;
    }

    public Build addExceptions(List<Exception> exceptions) {
      this.exceptions.addAll(exceptions);
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
        public List<Exception> exceptions() {
          return List.copyOf(exceptions);
        }
      };
    }
  }
}
