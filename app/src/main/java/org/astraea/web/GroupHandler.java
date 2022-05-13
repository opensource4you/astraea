package org.astraea.web;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.admin.Admin;

public class GroupHandler implements Handler {

  private final Admin admin;

  GroupHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public JsonObject response(Optional<String> target, Map<String, String> queries) {
    Predicate<Map.Entry<String, ?>> groupFilter =
        e -> target.stream().allMatch(t -> t.equals(e.getKey()));
    var topics = admin.topicNames();
    var consumerGroups = admin.consumerGroups();
    var offsets = admin.offsets(topics);

    var groups =
        consumerGroups.entrySet().stream()
            .filter(groupFilter)
            .map(
                cgAndTp ->
                    new Group(
                        cgAndTp.getKey(),
                        cgAndTp.getValue().assignment().entrySet().stream()
                            .map(
                                entry ->
                                    new Member(
                                        entry.getKey().memberId(),
                                        entry.getKey().groupInstanceId(),
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
    else if (target.isPresent())
      throw new NoSuchElementException("group: " + target.get() + " does not exist");
    else return new Groups(groups);
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
    private final String memberId;
    private final Optional<String> groupInstanceId;
    private final String clientId;
    private final String host;
    private final List<OffsetProgress> offsetProgress;

    Member(
        String memberId,
        Optional<String> groupInstanceId,
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
