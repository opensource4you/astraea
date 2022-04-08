package org.astraea.balancer.alpha;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.management.remote.JMXServiceURL;
import org.astraea.argument.Field;
import org.astraea.cost.CostFunction;
import org.astraea.metrics.collector.Fetcher;

public class Balancer {

  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final MetricCollector metricCollector;
  private final Set<CostFunction> registeredCostFunction;
  private final Map<CostFunction, Fetcher> registeredFetchers;
  private final ScheduledExecutorService scheduledExecutorService;
  private final RebalancePlanGenerator<Integer> rebalancePlanGenerator;

  public Balancer(Argument argument) {
    // initialize member variables
    this.jmxServiceURLMap = argument.jmxServiceURLMap;
    this.registeredCostFunction = Set.of();
    this.registeredFetchers =
        registeredCostFunction.stream()
            .collect(Collectors.toUnmodifiableMap(Function.identity(), CostFunction::fetcher));
    this.scheduledExecutorService = Executors.newScheduledThreadPool(8);

    // initialize main component
    this.metricCollector =
        new MetricCollector(
            this.jmxServiceURLMap, this.registeredFetchers.values(), this.scheduledExecutorService);
    // TODO: implement better plan generation
    this.rebalancePlanGenerator =
        (clusterNow, arguments) ->
            RebalancePlanProposal.builder()
                .noRebalancePlan()
                .withInfo(List.of("Bruh, no plan for you :3"))
                .build();
  }

  public void start() {
    this.metricCollector.start();

    // schedule a check for a period of time
    final long periodMs = Duration.ofMinutes(1).toMillis();
    this.scheduledExecutorService.scheduleWithFixedDelay(
        () -> {

          // TODO: implement the clusterInfo generation
          // dump metrics into cost function
          Map<CostFunction, Map<Integer, Double>> brokerScores =
              registeredCostFunction.parallelStream()
                  .map(costFunction -> Map.entry(costFunction, costFunction.cost(null)))
                  .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

          // print out current score
          printCostFunction(brokerScores);

          if (isClusterImbalance()) {
            final var proposal = rebalancePlanGenerator.generate(null, 0);

            if (proposal.isPlanGenerated()) {
              System.out.println("[New Rebalance Plan Generated] " + LocalDateTime.now());
              // TODO: describe the detail of this plan
              System.out.println(proposal.rebalancePlan().orElseThrow());
              // TODO: generate the rebalance commands
            } else {
              System.out.println("[No Rebalance Plan Generated] " + LocalDateTime.now());
            }

            // print info, warnings, exceptions
            System.out.println("[Information]");
            proposal.info().forEach(info -> System.out.printf(" * %s%n", info));
            System.out.println("[Warnings]");
            proposal.warnings().forEach(warning -> System.out.printf(" * %s%n", warning));
            IntStream.range(0, proposal.exceptions().size())
                .forEachOrdered(
                    index -> {
                      System.out.printf(
                          "[Exception %d/%d]%n", index + 1, proposal.exceptions().size());
                      proposal.exceptions().get(index).printStackTrace();
                    });
          }
        },
        periodMs,
        periodMs,
        TimeUnit.MILLISECONDS);
  }

  private void printCostFunction(Map<CostFunction, Map<Integer, Double>> brokerScores) {
    PrintStream printStream = System.out;
    brokerScores.forEach(
        (key, value) -> {
          printStream.printf("[%s]%n", key.getClass().getSimpleName());
          value.entrySet().stream()
              .sorted(Map.Entry.comparingByKey())
              .forEachOrdered(
                  entry ->
                      printStream.printf("Broker #%5d: %f%n", entry.getKey(), entry.getValue()));
          printStream.println();
        });
  }

  private boolean isClusterImbalance() {
    // TODO: Implement this
    return true;
  }

  public void stop() {
    this.metricCollector.close();
    this.scheduledExecutorService.shutdownNow();
  }

  public static void main(String[] args) {
    final Argument argument = org.astraea.argument.Argument.parse(new Argument(), args);
    new Balancer(argument).start();
  }

  static class Argument {

    @Parameter(
        names = {"--jmx.server.file"},
        description =
            "Path to a java properties file that contains all the jmxServiceUrl definitions and their corresponding broker.id",
        converter = JmxServiceUrlMappingFileField.class,
        required = true)
    Map<Integer, JMXServiceURL> jmxServiceURLMap;

    public static class JmxServiceUrlMappingFileField extends Field<Map<Integer, JMXServiceURL>> {

      static final Pattern serviceUrlKeyPattern =
          Pattern.compile("broker\\.(<brokerId>?[1-9][0-9]{0,4})");

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
                  + "no match for the following format :"
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
}
