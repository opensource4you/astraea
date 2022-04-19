package org.astraea.yunikorn.config;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class Limit {

  private String limit;

  private List<String> users;

  private List<String> group;

  private Map<String, String> maxresources;
  private BigInteger maxapplications;

  public void setUsers(List<String> users) {
    this.users = users;
  }

  public void setMaxapplications(BigInteger maxapplications) {
    this.maxapplications = maxapplications;
  }

  public void setLimit(String limit) {
    this.limit = limit;
  }

  public void setMaxresources(Map<String, String> maxresources) {
    this.maxresources = maxresources;
  }

  public void setGroup(List<String> group) {
    this.group = group;
  }

  public List<String> getUsers() {
    return users;
  }

  public BigInteger getMaxapplications() {
    return maxapplications;
  }

  public String getLimit() {
    return limit;
  }

  public Map<String, String> getMaxresources() {
    return maxresources;
  }

  public List<String> getGroup() {
    return group;
  }
}
