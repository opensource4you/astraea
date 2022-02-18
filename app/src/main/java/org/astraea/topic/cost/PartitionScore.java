package org.astraea.topic.cost;

import static org.astraea.argument.ArgumentUtil.parseArgument;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.BooleanConverter;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.BasicArgument;
import org.astraea.argument.validator.NotEmptyString;
import org.astraea.topic.TopicAdmin;

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

      partitionGood.sort(
          Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
      System.out.println(
          "The following partitions are balanced: "
              + partitionGood.stream()
                  .map(String::valueOf)
                  .collect(Collectors.joining(", ", "[", "]")));
    }
  }

  public static Map<Integer, Map<TopicPartition, Double>> execute(
      Argument argument, TopicAdmin admin) {
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
    var argument = parseArgument(new Argument(), args);
    var admin = TopicAdmin.of(argument.brokers);
    var score = execute(argument, admin);
    printScore(score, argument);
  }

  static class Argument extends BasicArgument {
    @Parameter(
        names = {"--exclude.internal.topics"},
        description =
            "True if you want to ignore internal topics like _consumer_offsets while counting score.",
        validateWith = NotEmptyString.class,
        converter = BooleanConverter.class)
    boolean excludeInternalTopic = false;

    @Parameter(
        names = {"--hide.balanced"},
        description = "True if you want to hide topics and partitions thar already balanced.",
        validateWith = NotEmptyString.class,
        converter = BooleanConverter.class)
    boolean hideBalanced = false;
  }
}
