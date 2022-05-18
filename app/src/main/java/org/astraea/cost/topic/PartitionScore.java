package org.astraea.cost.topic;

import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.admin.Admin;
import org.astraea.admin.TopicPartition;
import org.astraea.argument.BooleanField;

public class PartitionScore {
  public static void printScore(
      Map<Integer, Map<TopicPartition, Double>> score, Argument argument) {
    List<TopicPartition> partitionGood = new ArrayList<>();
    Map<Integer, Boolean> brokerGood = new HashMap<>();
    score
        .keySet()
        .forEach(
            broker -> {
              brokerGood.put(broker, true);
              score
                  .get(broker)
                  .keySet()
                  .forEach(
                      tp -> {
                        if (score.get(broker).get(tp) > 0) brokerGood.put(broker, false);
                      });

              if (!brokerGood.get(broker)) {
                System.out.println("\nbroker: " + broker);
                score
                    .get(broker)
                    .keySet()
                    .forEach(
                        tp -> {
                          if (score.get(broker).get(tp) > 0) {
                            System.out.println(tp + ": " + score.get(broker).get(tp));
                          } else {
                            partitionGood.add(tp);
                          }
                        });
              }
            });
    if (!argument.hideBalanced) {
      System.out.println(
          "\nThe following brokers are balanced: "
              + brokerGood.entrySet().stream()
                  .filter(Map.Entry::getValue)
                  .map(Map.Entry::getKey)
                  .collect(Collectors.toSet()));

      System.out.println(
          "The following partitions are balanced: "
              + partitionGood.stream()
                  .sorted()
                  .map(String::valueOf)
                  .collect(Collectors.joining(", ", "[", "]")));
    }
  }

  public static Map<Integer, Map<TopicPartition, Double>> execute(Argument argument, Admin admin) {
    var internalTopic =
        Set.of(
            "__consumer_offsets",
            "_confluent-command",
            "_confluent-metrics",
            "_confluent-telemetry-metrics",
            "__transaction_state");
    var brokerPartitionSize = GetPartitionInf.getSize(admin);
    var retentionMillis = GetPartitionInf.getRetentionMillis(admin);
    if (argument.excludeInternalTopic) internalTopic.forEach(retentionMillis::remove);
    var load = CalculateUtils.getLoad(brokerPartitionSize, retentionMillis);
    return CalculateUtils.getScore(load);
  }

  public static void main(String[] args) {
    var argument = org.astraea.argument.Argument.parse(new Argument(), args);
    var admin = Admin.of(argument.bootstrapServers());
    var score = execute(argument, admin);
    printScore(score, argument);
  }

  static class Argument extends org.astraea.argument.Argument {
    @Parameter(
        names = {"--exclude.internal.topics"},
        description =
            "True if you want to ignore internal topics like _consumer_offsets while counting score.",
        validateWith = BooleanField.class,
        converter = BooleanField.class)
    boolean excludeInternalTopic = false;

    @Parameter(
        names = {"--hide.balanced"},
        description = "True if you want to hide topics and partitions thar already balanced.",
        validateWith = BooleanField.class,
        converter = BooleanField.class)
    boolean hideBalanced = false;
  }
}
