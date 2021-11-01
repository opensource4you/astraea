package org.astraea.topic;

import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

public final class Group {
  private final String groupId;
  private final OptionalLong offset;
  private final List<Member> members;

  Group(String groupId, OptionalLong offset, List<Member> members) {
    this.groupId = groupId;
    this.offset = offset;
    this.members = Collections.unmodifiableList(members);
  }

  @Override
  public String toString() {
    return "Group{"
        + "groupId='"
        + groupId
        + '\''
        + ", offset="
        + (offset.isEmpty() ? "none" : offset.getAsLong())
        + ", members="
        + members
        + '}';
  }

  public String groupId() {
    return groupId;
  }

  public OptionalLong offset() {
    return offset;
  }

  public List<Member> members() {
    return members;
  }
}
