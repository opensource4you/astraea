package org.astraea.web;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.admin.Admin;

public class GroupHandler implements Handler {

  private final Admin admin;

  GroupHandler(Admin admin) {
    this.admin = admin;
  }

  Set<String> groupIds(Optional<String> target) {
    return Handler.compare(admin.consumerGroupIds(), target);
  }

  @Override
  public JsonObject get(Optional<String> target, Map<String, String> queries) {
    var topics = admin.topicNames();
    var consumerGroups = admin.consumerGroups(groupIds(target));
    var offsets = admin.offsets(topics);

    var groups =
        consumerGroups.entrySet().stream()
            .map(
                cgAndTp ->
                    new Group(
                        cgAndTp.getKey(),
                        cgAndTp.getValue().assignment().entrySet().stream()
                            .map(
                                entry ->
                                    new Member(
                                        entry.getKey().memberId(),
                                        // gson does not support Optional
                                        // see https://github.com/google/gson/issues/1102
                                        entry.getKey().groupInstanceId().orElse(null),
                                        entry.getKey().clientId(),
                                        entry.getKey().host(),
                                        entry.getValue().stream()
                                            .map(
                                                tp ->
                                                    offsets.containsKey(tp)
                                                            && cgAndTp
                                                                .getValue()
                                                                .consumeProgress()
                                                                .containsKey(tp)
                                                        ? Optional.of(
                                                            new OffsetProgress(
                                                                tp.topic(),
                                                                tp.partition(),
                                                                offsets.get(tp).earliest(),
                                                                cgAndTp
                                                                    .getValue()
                                                                    .consumeProgress()
                                                                    .get(tp),
                                                                offsets.get(tp).latest()))
                                                        : Optional.<OffsetProgress>empty())
                                            .filter(Optional::isPresent)
                                            .map(Optional::get)
                                            .collect(Collectors.toUnmodifiableList())))
                            .collect(Collectors.toUnmodifiableList())))
            .collect(Collectors.toUnmodifiableList());

    if (target.isPresent() && groups.size() == 1) return groups.get(0);
    return new Groups(groups);
  }

  static class OffsetProgress implements JsonObject {
    final String topicName;
    final int partitionId;
    final long earliest;
    final long current;
    final long latest;

    OffsetProgress(String topicName, int partitionId, long earliest, long current, long latest) {
      this.topicName = topicName;
      this.partitionId = partitionId;
      this.earliest = earliest;
      this.current = current;
      this.latest = latest;
    }
  }

  static class Member implements JsonObject {
    final String memberId;
    final String groupInstanceId;
    final String clientId;
    final String host;
    final List<OffsetProgress> offsetProgress;

    Member(
        String memberId,
        String groupInstanceId,
        String clientId,
        String host,
        List<OffsetProgress> offsetProgress) {
      this.memberId = memberId;
      this.groupInstanceId = groupInstanceId;
      this.clientId = clientId;
      this.host = host;
      this.offsetProgress = offsetProgress;
    }
  }

  static class Group implements JsonObject {
    final String groupId;
    final List<Member> members;

    Group(String groupId, List<Member> members) {
      this.groupId = groupId;
      this.members = members;
    }
  }

  static class Groups implements JsonObject {
    final List<Group> groups;

    Groups(List<Group> groups) {
      this.groups = groups;
    }
  }
}
