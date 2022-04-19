package org.astraea.yunikorn.config;

public class PlacementRule {
  private String name;
  private Boolean create;
  private Fliter filter;
  private PlacementRule parent;
  private String value;

  public void setName(String name) {
    this.name = name;
  }

  public void setCreate(Boolean create) {
    this.create = create;
  }

  public void setFilter(Fliter filter) {
    this.filter = filter;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void setParent(PlacementRule parent) {
    this.parent = parent;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public Boolean getCreate() {
    return create;
  }

  public PlacementRule getParent() {
    return parent;
  }

  public Fliter getFilter() {
    return filter;
  }
}
