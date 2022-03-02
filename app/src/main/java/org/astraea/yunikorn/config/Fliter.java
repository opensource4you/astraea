package org.astraea.yunikorn.config;


import java.util.List;

public class Fliter {
    private String type;
    private List<String> users ;
    private List<String> groups ;
    public String getType(){
        return this.type;
    }
    public List<String> getUsers(){
        return this.users;
    }
    public List<String> getGroups(){
        return this.groups;
    }

    public void setUsers(List<String> users) {
        this.users = users;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }
}
