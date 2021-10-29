package org.astraea.topic;

import java.util.Optional;

public final class Member {
  private final String memberId;
  private final Optional<String> groupInstanceId;
  private final String clientId;
  private final String host;

  Member(String memberId, Optional<String> groupInstanceId, String clientId, String host) {
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
}
