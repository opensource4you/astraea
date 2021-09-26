package org.astraea.offset;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

public class OffsetExplorer {
  static final String BROKERS_KEY = "--bootstrap.servers";
  static final String TOPIC_KEY = "--topic";

  interface Admin extends Closeable {
    Set<String> topics();

    Set<TopicPartition> partitions(Set<String> topics);

    Map<TopicPartition, Long> earliestOffsets(Set<TopicPartition> partitions);

    Map<TopicPartition, Long> latestOffsets(Set<TopicPartition> partitions);

    static Admin of(org.apache.kafka.clients.admin.Admin admin) {
      return new Admin() {
        @Override
        public void close() {
          admin.close();
        }

        @Override
        public Set<String> topics() {
          try {
            return admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Set<TopicPartition> partitions(Set<String> topics) {
          try {
            return admin.describeTopics(topics).all().get().entrySet().stream()
                .flatMap(
                    e ->
                        e.getValue().partitions().stream()
                            .map(p -> new TopicPartition(e.getKey(), p.partition())))
                .collect(Collectors.toSet());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Map<TopicPartition, Long> earliestOffsets(Set<TopicPartition> partitions) {
          try {
            return admin
                .listOffsets(
                    partitions.stream()
                        .collect(Collectors.toMap(e -> e, e -> new OffsetSpec.EarliestSpec())))
                .all()
                .get()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Map<TopicPartition, Long> latestOffsets(Set<TopicPartition> partitions) {
          try {
            return admin
                .listOffsets(
                    partitions.stream()
                        .collect(Collectors.toMap(e -> e, e -> new OffsetSpec.LatestSpec())))
                .all()
                .get()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
  }

  private static String help() {
    return BROKERS_KEY
        + "REQUIRED: The server to connect to"
        + TOPIC_KEY
        + " OPTIONAL: the topic to check";
  }

  static Map<TopicPartition, Map.Entry<Long, Long>> execute(Admin admin, Set<String> topics) {
    var topicPartitions = admin.partitions(topics);

    var earliestOffsets = admin.earliestOffsets(topicPartitions);
    var latestOffsets = admin.latestOffsets(topicPartitions);

    var result = new LinkedHashMap<TopicPartition, Map.Entry<Long, Long>>();

    topics.forEach(
        topic ->
            earliestOffsets.entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topic))
                .sorted(
                    Comparator.comparing((Map.Entry<TopicPartition, Long> o) -> o.getKey().topic())
                        .thenComparingInt(o -> o.getKey().partition()))
                .forEach(
                    earliestOffset ->
                        latestOffsets.entrySet().stream()
                            .filter(e -> e.getKey().equals(earliestOffset.getKey()))
                            .forEach(
                                latestOffset ->
                                    result.put(
                                        earliestOffset.getKey(),
                                        Map.entry(
                                            earliestOffset.getValue(), latestOffset.getValue())))));

    return result;
  }

  public static void main(String[] args) throws IOException {
    var configs = toMaps(args);
    try (var admin = Admin.of(AdminClient.create(toAdminProps(configs)))) {
      var topics =
          configs.containsKey(TOPIC_KEY)
              ? Collections.singleton(configs.get(TOPIC_KEY))
              : admin.topics();

      var result = execute(admin, topics);
      result.forEach(
          (k, v) ->
              System.out.println(
                  "topic: "
                      + k.topic()
                      + " partition: "
                      + k.partition()
                      + " start: "
                      + v.getKey()
                      + " end: "
                      + v.getValue()));
    }
  }

  static Map<String, Object> toAdminProps(Map<String, String> argMap) {
    var props = new HashMap<String, Object>();
    if (!argMap.containsKey(BROKERS_KEY)) throw new IllegalArgumentException(help());
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, argMap.get(BROKERS_KEY));
    return props;
  }

  private static Map<String, String> toMaps(String[] args) {
    var argMap = new HashMap<String, String>();
    for (var i = 0; i <= args.length; i += 2) {
      if (i + 1 >= args.length) break;
      argMap.put(args[i], args[i + 1]);
    }
    return argMap;
  }
}
