package org.astraea.yunikorn.config;

import java.util.List;

public class Fliter {
  private String type;
  private List<String> users;
  private List<String> groups;

  public void setType(String type) {
    this.type = type;
  }

  public void setGroups(List<String> groups) {
    this.groups = groups;
  }

  public void setUsers(List<String> users) {
    this.users = users;
  }

  public String getType() {
    return type;
  }

  public List<String> getGroups() {
    return groups;
  }

  public List<String> getUsers() {
    return users;
  }
}
