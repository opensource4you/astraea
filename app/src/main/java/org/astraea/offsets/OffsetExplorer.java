package org.astraea.offsets;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

public class OffsetExplorer {
  static final String BROKERS_KEY = "--bootstrap.servers";
  static final String TOPIC_KEY = "--topic";

  private static String help() {
    return BROKERS_KEY
        + "REQUIRED: The server to connect to"
        + TOPIC_KEY
        + " OPTIONAL: the topic to check";
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    var configs = toMaps(args);
    try (var client = AdminClient.create(toAdminProps(configs))) {
      var topics =
          configs.containsKey(TOPIC_KEY)
              ? Collections.singleton(configs.get(TOPIC_KEY))
              : client.listTopics(new ListTopicsOptions().listInternal(true)).names().get();

      var topicPartitions =
          client.describeTopics(topics).all().get().entrySet().stream()
              .flatMap(
                  e ->
                      e.getValue().partitions().stream()
                          .map(p -> new TopicPartition(e.getKey(), p.partition())))
              .collect(Collectors.toList());

      var earliestOffsets =
          client
              .listOffsets(
                  topicPartitions.stream()
                      .collect(Collectors.toMap(e -> e, e -> new OffsetSpec.EarliestSpec())))
              .all()
              .get();
      var latestOffsets =
          client
              .listOffsets(
                  topicPartitions.stream()
                      .collect(Collectors.toMap(e -> e, e -> new OffsetSpec.LatestSpec())))
              .all()
              .get();

      topics.forEach(
          topic ->
              earliestOffsets.entrySet().stream()
                  .filter(e -> e.getKey().topic().equals(topic))
                  .sorted(
                      Comparator.comparing(
                              (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>
                                      o) -> o.getKey().topic())
                          .thenComparingInt(o -> o.getKey().partition()))
                  .forEach(
                      earliestOffset ->
                          latestOffsets.entrySet().stream()
                              .filter(e -> e.getKey().equals(earliestOffset.getKey()))
                              .forEach(
                                  latestOffset ->
                                      System.out.println(
                                          "topic: "
                                              + earliestOffset.getKey().topic()
                                              + " partition: "
                                              + earliestOffset.getKey().partition()
                                              + " start: "
                                              + earliestOffset.getValue().offset()
                                              + " end: "
                                              + latestOffset.getValue().offset()))));
    }
  }

  private static Map<String, Object> toAdminProps(Map<String, String> argMap) {
    var props = new HashMap<String, Object>();
    props.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        argMap.computeIfAbsent(
            BROKERS_KEY,
            a -> {
              throw new IllegalArgumentException(help());
            }));
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
