package org.astraea.yunikorn.config;

import java.util.HashMap;
import java.util.Map;
import lombok.*;
@Getter @Setter
public class NodeSortingPolicy {
    public static final String CORE_KEY = "vcore";
    public static final String MEMORY_KEY = "memory";
    private String type ;
    private Map<String, Double> resourceweights = new HashMap<>();
    NodeSortingPolicy(){
        resourceweights.put(MEMORY_KEY, 1.0);
        resourceweights.put(CORE_KEY, 1.0);

    }
    public void putResourceweights (String resourceType, Double resourceweights){
        this.resourceweights.put(resourceType, resourceweights);
    }

}
