package org.astraea.yunikorn.config;

public class PlacementRule {
    private String name;
    private Boolean create ;
    private Fliter filter;
    private PlacementRule parent;
    private String value ;

    public void setName(String name) {
        this.name = name;
    }

    public void setParent(PlacementRule parent) {
        this.parent = parent;
    }

    public String getName() {
        return name;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setFilter(Fliter filter) {
        this.filter = filter;
    }

    public void setCreate(Boolean create) {
        this.create = create;
    }

    public PlacementRule getParent() {
        return parent;
    }

    public Boolean getCreate() {
        return create;
    }

    public Fliter getFilter() {return filter;
    }

    public String getValue() {
        return value;
    }
}
