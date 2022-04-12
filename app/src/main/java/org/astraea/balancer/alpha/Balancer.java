package org.astraea.balancer.alpha;

import static org.astraea.balancer.alpha.BalancerUtils.clusterSnapShot;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.Utils;
import org.astraea.argument.Field;
import org.astraea.balancer.alpha.cost.FolderSizeCost;
import org.astraea.balancer.alpha.generator.ShufflePlanGenerator;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.CostFunction;
import org.astraea.cost.LoadCost;
import org.astraea.cost.MemoryWarningCost;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.PartitionInfo;
import org.astraea.metrics.HasBeanObject;
import org.astraea.topic.TopicAdmin;

public class Balancer implements Runnable {

  private final Argument argument;
  private final Thread balancerThread;
  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final MetricCollector metricCollector;
  private final Set<CostFunction> registeredBrokerCostFunction;
  private final Set<org.astraea.balancer.alpha.cost.CostFunction>
      registeredTopicPartitionCostFunction;
  private final ScheduledExecutorService scheduledExecutorService;
  private final RebalancePlanGenerator rebalancePlanGenerator;
  private final TopicAdmin topicAdmin;

  public Balancer(Argument argument) {
    // initialize member variables
    this.argument = argument;
    this.jmxServiceURLMap = argument.jmxServiceURLMap;
    this.registeredBrokerCostFunction = Set.of(new LoadCost(), new MemoryWarningCost());
    this.registeredTopicPartitionCostFunction = Set.of(new FolderSizeCost());
    this.scheduledExecutorService = Executors.newScheduledThreadPool(8);

    // initialize main component
    this.balancerThread = new Thread(this);
    this.metricCollector =
        new MetricCollector(
            this.jmxServiceURLMap,
            this.registeredBrokerCostFunction.stream()
                .map(CostFunction::fetcher)
                .collect(Collectors.toUnmodifiableList()),
            this.scheduledExecutorService);
    this.topicAdmin = TopicAdmin.of(argument.props());
    this.rebalancePlanGenerator = new ShufflePlanGenerator(2, 5);
  }

  public void start() {
    balancerThread.start();
  }

  public void run() {
    this.metricCollector.start();

    // schedule a check for a period of time
    final long periodMs = Duration.ofSeconds(30).toMillis();
    while (!Thread.interrupted()) {
      // generate cluster info
      final var clusterInfo =
          ClusterInfo.of(clusterSnapShot(topicAdmin), metricCollector.fetchMetrics());

      // dump metrics into cost function
      Map<CostFunction, Map<Integer, Double>> currentBrokerCost =
          registeredBrokerCostFunction.parallelStream()
              .map(costFunction -> Map.entry(costFunction, costFunction.cost(clusterInfo)))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      Map<org.astraea.balancer.alpha.cost.CostFunction, Map<TopicPartitionReplica, Double>>
          currentTopicPartitionCost =
              registeredTopicPartitionCostFunction.parallelStream()
                  .map(
                      costFunction ->
                          Map.entry(
                              costFunction,
                              Utils.handleException(() -> costFunction.cost(clusterInfo))))
                  .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

      // print out current score
      BalancerUtils.printCost(currentBrokerCost);
      BalancerUtils.printTopicPartitionReplicaCost(currentTopicPartitionCost);

      final var rankedProposal =
          new TreeSet<ScoredProposal>(Comparator.comparingDouble(x -> x.score));

      final int iteration = 1000;
      for (int i = 0; i < iteration; i++) {
        final var proposal = rebalancePlanGenerator.generate(clusterInfo);
        final var proposedClusterInfo = clusterInfoFromProposal(clusterInfo, proposal);

        final var estimatedBrokerCost =
            registeredBrokerCostFunction.parallelStream()
                .map(
                    costFunction -> Map.entry(costFunction, costFunction.cost(proposedClusterInfo)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        final var estimatedTPRCost =
            registeredTopicPartitionCostFunction.parallelStream()
                .map(
                    costFunction ->
                        Map.entry(
                            costFunction,
                            Utils.handleException(() -> costFunction.cost(proposedClusterInfo))))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        final var estimatedCostSum = costSum(estimatedBrokerCost, estimatedTPRCost);

        rankedProposal.add(new ScoredProposal(estimatedCostSum, proposal));
        while (rankedProposal.size() > 5) rankedProposal.pollLast();
      }

      final var selectedProposal = rankedProposal.first();
      final var currentCostSum = costSum(currentBrokerCost, currentTopicPartitionCost);
      final var proposedCostSum = selectedProposal.score;
      if (proposedCostSum < currentCostSum) {
        System.out.println("[New Proposal Found]");
        System.out.println("Current cost sum: " + currentCostSum);
        System.out.println("Proposed cost sum: " + proposedCostSum);
        BalancerUtils.describeProposal(
            selectedProposal.proposal, BalancerUtils.currentAllocation(topicAdmin, clusterInfo));
      } else {
        System.out.println("[No Usable Proposal Found]");
        System.out.println("Current cost sum: " + currentCostSum);
        System.out.println("Best proposed cost sum calculated: " + proposedCostSum);
      }

      try {
        TimeUnit.MILLISECONDS.sleep(periodMs);
      } catch (InterruptedException e) {
        e.printStackTrace();
        break;
      }
    }
  }

  /** create a fake cluster info based on given proposal */
  private ClusterInfo clusterInfoFromProposal(
      ClusterInfo clusterInfo, RebalancePlanProposal proposal) {
    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return clusterInfo.nodes();
      }

      @Override
      public List<PartitionInfo> availablePartitions(String topic) {
        return partitions(topic).stream()
            .filter(x -> x.leader() != null)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Set<String> topics() {
        return proposal
            .rebalancePlan()
            .map(clusterLogAllocation -> clusterLogAllocation.allocation().keySet())
            .orElseGet(clusterInfo::topics);
      }

      @Override
      public List<PartitionInfo> partitions(String topic) {
        return proposal
            .rebalancePlan()
            .map(
                clusterLogAllocation ->
                    clusterLogAllocation.allocation().get(topic).entrySet().stream()
                        .map(
                            entry -> {
                              var collect =
                                  entry.getValue().stream()
                                      .map(x -> NodeInfo.of(x, "", 0))
                                      .collect(Collectors.toUnmodifiableList());
                              return PartitionInfo.of(
                                  topic, entry.getKey(), collect.get(0), collect);
                            })
                        .collect(Collectors.toUnmodifiableList()))
            .orElse(clusterInfo.partitions(topic));
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return clusterInfo.beans(brokerId);
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return clusterInfo.allBeans();
      }
    };
  }

  /**
   * Given a final score for this all the cost function results, the value will be a non-negative
   * real number. Basically, 0 means the most ideal state given all the cost functions.
   */
  private double costSum(
      Map<CostFunction, Map<Integer, Double>> costOfProposal,
      Map<org.astraea.balancer.alpha.cost.CostFunction, Map<TopicPartitionReplica, Double>>
          costOfTPR) {
    // TODO: replace this with a much more meaningful implementation
    var a =
        costOfProposal.values().stream()
            .flatMapToDouble(x -> x.values().stream().mapToDouble(Double::doubleValue))
            .average()
            .orElse(0);
    var b =
        costOfTPR.values().stream()
            .flatMapToDouble(x -> x.values().stream().mapToDouble(Double::doubleValue))
            .average()
            .orElse(0);
    return (a + b) / 2;
  }

  public void stop() {
    this.metricCollector.close();
    this.scheduledExecutorService.shutdownNow();
  }

  public static void main(String[] args) throws InterruptedException {
    final Argument argument = org.astraea.argument.Argument.parse(new Argument(), args);
    final Balancer balancer = new Balancer(argument);
    balancer.start();
    balancer.balancerThread.join();
    balancer.stop();
  }

  static class Argument extends org.astraea.argument.Argument {

    @Parameter(
        names = {"--jmx.server.file"},
        description =
            "Path to a java properties file that contains all the jmxServiceUrl definitions and their corresponding broker.id",
        converter = JmxServiceUrlMappingFileField.class,
        required = true)
    Map<Integer, JMXServiceURL> jmxServiceURLMap;

    public static class JmxServiceUrlMappingFileField extends Field<Map<Integer, JMXServiceURL>> {

      static final Pattern serviceUrlKeyPattern =
          Pattern.compile("broker\\.(?<brokerId>[1-9][0-9]{0,9})");

      static Map.Entry<Integer, JMXServiceURL> transformEntry(Map.Entry<String, String> entry) {
        final Matcher matcher = serviceUrlKeyPattern.matcher(entry.getKey());
        if (matcher.matches()) {
          try {
            int brokerId = Integer.parseInt(matcher.group("brokerId"));
            final JMXServiceURL jmxServiceURL = new JMXServiceURL(entry.getValue());
            return Map.entry(brokerId, jmxServiceURL);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad integer format for " + entry.getKey(), e);
          } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                "Bad JmxServiceURL format for " + entry.getValue(), e);
          }
        } else {
          throw new IllegalArgumentException(
              "Bad key format for "
                  + entry.getKey()
                  + " no match for the following format :"
                  + serviceUrlKeyPattern.pattern());
        }
      }

      @Override
      public Map<Integer, JMXServiceURL> convert(String value) {
        final Properties properties = new Properties();

        try (var reader = Files.newBufferedReader(Path.of(value))) {
          properties.load(reader);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        return properties.entrySet().stream()
            .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
            .map(
                entry -> {
                  try {
                    return transformEntry(entry);
                  } catch (Exception e) {
                    throw new IllegalArgumentException(
                        "Failed to process JMX service URL map:" + value, e);
                  }
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    }
  }

  private static class ScoredProposal {
    private final double score;
    private final RebalancePlanProposal proposal;

    private ScoredProposal(double score, RebalancePlanProposal proposal) {
      this.score = score;
      this.proposal = proposal;
    }
  }
}
