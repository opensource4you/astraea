package org.astraea.app.admin;

import java.util.Objects;
import java.util.Optional;

public final class Member {
  private final String groupId;
  private final String memberId;
  private final Optional<String> groupInstanceId;
  private final String clientId;
  private final String host;

  Member(
      String groupId,
      String memberId,
      Optional<String> groupInstanceId,
      String clientId,
      String host) {
    this.groupId = groupId;
    this.memberId = memberId;
    this.groupInstanceId = groupInstanceId;
    this.clientId = clientId;
    this.host = host;
  }

  @Override
  public String toString() {
    return "Member{"
        + "memberId='"
        + memberId
        + '\''
        + ", groupInstanceId="
        + groupInstanceId
        + ", clientId='"
        + clientId
        + '\''
        + ", host='"
        + host
        + '\''
        + '}';
  }

  public String groupId() {
    return groupId;
  }

  public String memberId() {
    return memberId;
  }

  public Optional<String> groupInstanceId() {
    return groupInstanceId;
  }

  public String clientId() {
    return clientId;
  }

  public String host() {
    return host;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Member member = (Member) o;
    return Objects.equals(groupId, member.groupId)
        && Objects.equals(memberId, member.memberId)
        && Objects.equals(groupInstanceId, member.groupInstanceId)
        && Objects.equals(clientId, member.clientId)
        && Objects.equals(host, member.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, memberId, groupInstanceId, clientId, host);
  }
}
