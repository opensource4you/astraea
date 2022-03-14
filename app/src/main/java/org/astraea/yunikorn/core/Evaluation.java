package org.astraea.yunikorn.core;

import java.util.HashMap;
import java.util.Map;
import org.astraea.yunikorn.config.NodeSortingPolicy;

public class Evaluation {
  private Map<String, String> unused = new HashMap<>();
  private Map<String, String> resources = new HashMap<>();

  public Evaluation() {
    this.resources.put(NodeSortingPolicy.MEMORY_KEY, "1.0");
    this.resources.put(NodeSortingPolicy.CORE_KEY, "1.0");
    this.unused.put(NodeSortingPolicy.MEMORY_KEY, "1.0");
    this.unused.put(NodeSortingPolicy.CORE_KEY, "1.0");
  }

  public void setResources(String memory, String vcore) {
    this.resources.put(NodeSortingPolicy.MEMORY_KEY, memory);
    this.resources.put(NodeSortingPolicy.CORE_KEY, vcore);
  }

  public void setUnused(String memory, String vcore) {
    this.unused.put(NodeSortingPolicy.MEMORY_KEY, memory);
    this.unused.put(NodeSortingPolicy.CORE_KEY, vcore);
  }

  public double calculate() {
    double resourceweight = 1;
    resourceweight =
        Double.valueOf(resources.get(NodeSortingPolicy.MEMORY_KEY))
            / Double.valueOf(resources.get(NodeSortingPolicy.CORE_KEY));

    if (unused.get("vcore") != "0") {
      resourceweight =
          (Double.valueOf(unused.get(NodeSortingPolicy.MEMORY_KEY))
                  / Double.valueOf(unused.get(NodeSortingPolicy.CORE_KEY)))
              / resourceweight;
    } else return 1;

    return resourceweight;
  }

  public void setUnused(Map<String, String> unused) {
    this.unused = unused;
  }
}
