package org.astraea.yunikorn.config;

import java.util.List;
import java.util.Map;

public class QueueConfig {
    private String name;
    private Boolean parent;
    private Resources resources ;
    private Map<String, String> properties ;
    private long maxapplications;
    private String adminacl ;
    private String submitacl;
    private ChildTemplate childtemplate ;
    private List<QueueConfig> queues ;
    private List<Limit> limits ;

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public void setParent(Boolean parent) {
        this.parent = parent;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<QueueConfig> getQueues() {
        return queues;
    }

    public long getMaxapplications() {
        return maxapplications;
    }

    public Boolean getParent() {
        return parent;
    }

    public ChildTemplate getChildtemplate() {
        return childtemplate;
    }

    public List<Limit> getLimits() {
        return limits;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Resources getResources() {
        return resources;
    }

    public String getAdminacl() {
        return adminacl;
    }

    public String getSubmitacl() {
        return submitacl;
    }

    public void setAdminacl(String adminacl) {
        this.adminacl = adminacl;
    }

    public void setChildtemplate(ChildTemplate childtemplate) {
        this.childtemplate = childtemplate;
    }

    public void setLimits(List<Limit> limits) {
        this.limits = limits;
    }

    public void setQueues(List<QueueConfig> queues) {
        this.queues = queues;
    }

    public void setMaxapplications(long maxapplications) {
        this.maxapplications = maxapplications;
    }

    public void setResources(Resources resources) {
        this.resources = resources;
    }

    public void setSubmitacl(String submitacl) {
        this.submitacl = submitacl;
    }
}
