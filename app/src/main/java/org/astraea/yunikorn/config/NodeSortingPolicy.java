package org.astraea.yunikorn.config;

import java.util.HashMap;
import java.util.Map;


public class NodeSortingPolicy {
  public static final String CORE_KEY = "vcore";
  public static final String MEMORY_KEY = "memory";
  private String type;
  private Map<String, Double> resourceweights = new HashMap<>();

  NodeSortingPolicy() {
    resourceweights.put(MEMORY_KEY, 1.0);
    resourceweights.put(CORE_KEY, 1.0);
  }

  public void putResourceweights(String resourceType, Double resourceweights) {
    this.resourceweights.put(resourceType, resourceweights);
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setResourceweights(Map<String, Double> resourceweights) {
    this.resourceweights = resourceweights;
  }

  public String getType() {
    return type;
  }

  public Map<String, Double> getResourceweights() {
    return resourceweights;
  }
}
