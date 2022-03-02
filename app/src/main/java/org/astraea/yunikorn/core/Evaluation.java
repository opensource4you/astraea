package org.astraea.yunikorn.core;

import java.util.*;

import static java.util.Map.*;

public class Evaluation {
    private Map<String, String> unused = new HashMap<>();
    private Map<String, String> resources = new HashMap<>();

    public Evaluation() {
        this.resources.put("memory", "1.0");
        this.resources.put("vcore", "1.0");
        this.unused.put("memory", "1.0");
        this.unused.put("vcore", "1.0");
    }

    public void setResources(String memory, String vcore) {
        this.resources.put("memory", memory);
        this.resources.put("vcore", vcore);
    }

    public Map<String, String> getResources() {
        return resources;
    }

    public void setUnused(String memory, String vcore) {
        this.unused.put("memory", memory);
        this.unused.put("vcore", vcore);
    }



    public double calculate(){
        double resourceweight = 1;
        resourceweight = Double.valueOf(resources.get("memory"))/Double.valueOf(resources.get("vcore"));

        if (unused.get("vcore")!="0"){
            resourceweight = (Double.valueOf(unused.get("memory"))/ Double.valueOf(unused.get("vcore")))/resourceweight;
        }
        else
            return 1;

        return resourceweight;



    }

    public void setUnused(Map<String, String> unused) {
        this.unused = unused;
    }
}
