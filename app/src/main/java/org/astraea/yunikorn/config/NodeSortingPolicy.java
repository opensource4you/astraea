package org.astraea.yunikorn.config;

import java.util.HashMap;
import java.util.Map;

public class NodeSortingPolicy {
    private String type ;
    private Map<String, Double> resourceweights = new HashMap<>();
    public String getType(){
        return this.type;
    }
    NodeSortingPolicy(){
        resourceweights.put("memory", 1.0);
        resourceweights.put("vcore", 1.0);

    }

    public void setType(String type) {
        this.type = type;
    }

    public void putResourceweights (String resourceType, Double resourceweights){
        this.resourceweights.put(resourceType, resourceweights);
    }
    public Map<String, Double> getResourceweights() {
        return resourceweights;
    }

    public void setResourceweights(Map<String, Double> resourceweights) {
        this.resourceweights = resourceweights;
    }
}
