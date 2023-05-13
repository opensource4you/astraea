/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.benchmark;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.argument.Argument;
import org.astraea.app.argument.PathField;
import org.astraea.app.argument.PositiveIntegerField;
import org.astraea.app.benchmark.balancer.BalancerBenchmark;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.VersionUtils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.BalancerProblemFormat;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.json.JsonConverter;
import org.astraea.common.json.TypeRef;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;

public class BalancerBenchmarkApp {

  public void execute(String[] args) {
    var name = args.length > 0 ? args[0] : "help";
    var arguments = Arrays.stream(args).skip(1).toArray(String[]::new);
    var benchmarks =
        Map.<String, Runnable>of(
            "experiment",
            () -> runExperiment(Argument.parse(new ExperimentArgument(), arguments)),
            "cost_profiling",
            () -> runCostProfiling(Argument.parse(new CostProfilingArgument(), arguments)));

    Runnable help =
        () -> {
          System.out.printf("Usage: %s [args ...]%n", benchmarks.keySet());
          if (!name.equalsIgnoreCase("help"))
            throw new IllegalArgumentException("Unknown benchmark name: " + name);
        };

    // try to run the specified benchmark or cry for help
    benchmarks.getOrDefault(name, help).run();
  }

  void runExperiment(ExperimentArgument argument) {
    var cluster = argument.fetchClusterInfo();
    var beans = argument.fetchClusterBean();
    var problem = argument.fetchBalancerProblem();
    var balancer =
        Utils.construct(
            (Class<Balancer>) Utils.packException(() -> Class.forName(problem.balancer)),
            Configuration.of(problem.balancerConfig));

    System.out.println(optimizationSummary(argument, cluster, beans, problem));

    var result =
        BalancerBenchmark.experiment()
            .setBalancer(balancer)
            .setClusterInfo(cluster)
            .setClusterBean(beans)
            .setAlgorithmConfig(problem.parse())
            .setExperimentTrials(argument.trials)
            .setExecutionTimeout(problem.timeout)
            .start()
            .toCompletableFuture()
            .join();

    System.out.println(experimentSummary(result));
  }

  void runCostProfiling(CostProfilingArgument argument) {
    var cluster = argument.fetchClusterInfo();
    var beans = argument.fetchClusterBean();
    var problem = argument.fetchBalancerProblem();
    var balancer =
        Utils.construct(
            (Class<Balancer>) Utils.packException(() -> Class.forName(problem.balancer)),
            Configuration.of(problem.balancerConfig));

    System.out.println(optimizationSummary(argument, cluster, beans, problem));

    var result =
        BalancerBenchmark.costProfiling()
            .setBalancer(balancer)
            .setClusterInfo(cluster)
            .setClusterBean(beans)
            .setAlgorithmConfig(problem.parse())
            .setExecutionTimeout(problem.timeout)
            .start()
            .toCompletableFuture()
            .join();

    System.out.println(costProfilingSummary(argument, result));
  }

  private String optimizationSummary(
      CommonArgument arg, ClusterInfo info, ClusterBean bean, BalancerProblemFormat optimization) {
    var format =
        """
        Balancer Benchmark
        ===============================

        * Version: %s
        * Build Time: %s
        * Revision: %s
        * Author: %s

        ## Balancing Problem

        ```json
        %s
        ```

        * Execution: %s
        * Balancer: %s
        * Balancer Configuration:
        %s
        * Cluster Cost Function: %s
        * Move Cost Function: %s
        * Cost Function Configuration:
        %s

        ## ClusterInfo Summary

        * ClusterId: %s
        * Topics: %d
        * Partition: %d
        * Replicas: %d
        * Broker Count: %d

        ## ClusterBean Summary

        * Total Metrics: %d
        * Avg Metrics Per Broker: %f
        * Broker Count: %d
        * Metrics Start From: %s
        * Metrics End at: %s
        * Recorded Duration: %s

        """;

    var metricStart =
        bean.all().values().stream()
            .flatMap(Collection::stream)
            .map(HasBeanObject::beanObject)
            .mapToLong(BeanObject::createdTimestamp)
            .min()
            .stream()
            .mapToObj(
                time ->
                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault())
                        .toLocalDateTime())
            .findFirst()
            .orElse(null);
    var metricEnd =
        bean.all().values().stream()
            .flatMap(Collection::stream)
            .map(HasBeanObject::beanObject)
            .mapToLong(BeanObject::createdTimestamp)
            .max()
            .stream()
            .mapToObj(
                time ->
                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault())
                        .toLocalDateTime())
            .findFirst()
            .orElse(null);
    var duration =
        metricStart != null && metricEnd != null
            ? Duration.between(metricStart, metricEnd)
            : Duration.ZERO;

    return String.format(
        format,
        // astraea version
        VersionUtils.VERSION,
        VersionUtils.DATE,
        VersionUtils.REVISION,
        VersionUtils.BUILDER,
        // Balancer Problem Summary
        arg.fetchBalancerProblemJson(),
        optimization.timeout,
        optimization.balancer,
        Optional.of(
                optimization.balancerConfig.entrySet().stream()
                    .map(e -> String.format("  * \"%s\": %s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(System.lineSeparator())))
            .filter(Predicate.not(String::isEmpty))
            .orElse("  * no config"),
        optimization.parse().clusterCostFunction().toString(),
        optimization.parse().moveCostFunction().toString(),
        Optional.of(
                optimization.costConfig.entrySet().stream()
                    .map(e -> String.format("  * \"%s\": %s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(System.lineSeparator())))
            .filter(Predicate.not(String::isEmpty))
            .orElse("  * no config"),
        // ClusterInfo Summary
        info.clusterId(),
        info.topicNames().size(),
        info.topicPartitions().size(),
        info.replicas().size(),
        info.brokers().size(),
        // ClusterBean Summary
        bean.all().values().stream().mapToInt(Collection::size).sum(),
        (double) bean.all().values().stream().mapToInt(Collection::size).sum()
            / bean.brokerIds().size(),
        bean.brokerIds().size(),
        metricStart != null ? metricStart : "no metric",
        metricEnd != null ? metricEnd : "no metric",
        duration);
  }

  private String experimentSummary(BalancerBenchmark.ExperimentResult result) {
    var format =
        """
        Balancer Experiment Result
        ===============================

        * Attempted Trials: %d
        * Solution Found Trials: %d
        * No Solution Found Trials: %d

        ## ClusterCost Detail

        * Initial ClusterCost: %f
          > %s
        * Best ClusterCost: %s
          > %s

        ## Statistics

        * Initial Cost: %f
        * Min Cost: %s
        * Average Cost: %s
        * Max Cost: %s
        * Cost Variance: %s

        ## All Cost Values

        ```
        %s
        ```

        """;

    var count = result.costSummary().getCount();

    return String.format(
        format,
        // Trials
        result.trials(),
        result.costs().size(),
        result.trials() - result.costs().size(),
        // Cost Detail
        result.initial().value(),
        result.initial(),
        result
            .bestCost()
            .map(ClusterCost::value)
            .map(Object::toString)
            .orElse("no usable solution found"),
        result.bestCost().map(Object::toString).orElse("no usable solution found"),
        // Cost Statistics
        result.initial().value(),
        count > 0 ? result.costSummary().getMin() : -1,
        count > 0 ? result.costSummary().getAverage() : -1,
        count > 0 ? result.costSummary().getMax() : -1,
        result.variance().orElse(-1),
        // All values
        result.costs().stream()
            .mapToDouble(ClusterCost::value)
            .sorted()
            .mapToObj(Double::toString)
            .collect(Collectors.joining(System.lineSeparator())));
  }

  private String costProfilingSummary(
      CostProfilingArgument arg, BalancerBenchmark.CostProfilingResult result) {
    var format =
        """
        Balancer Cost Profiling Result
        ===============================

        * Initial Cost Value: %f
          > %s

        * Best Cost Value: %s
          > %s

        ## Runtime Statistics

        * Execution Time: %s
        * Average Iteration Time: %.3f ms
        * Average Balancer Operation Time: %.3f ms
        * Average ClusterCost Processing Time: %.3f ms
        * Average MoveCost Processing Time: %.3f ms
        * Total ClusterCost Evaluation: %d
        * Total MoveCost Evaluation: %d

        ## Detail

        * Cost Profiling Result (ClusterCost Only) in CSV: %s
        * Cost Profiling Result (All) in CSV: %s
        """;

    // use the move cost evaluation count as the number of iteration(an optimization attempt) been
    // performed. we are not using cluster cost since some balancer implementation won't perform
    // cluster cost evaluation if it knows the solution is infeasible.
    var iterations = Math.max(1, result.moveCostProcessingTimeNs().getCount());
    var time = System.currentTimeMillis();
    var randomName = Utils.randomString(4);

    var csvClusterCostFilename = "cost-profiling-" + time + "-" + randomName + ".csv";
    var csv0 = Path.of(arg.exportFolder.toAbsolutePath().toString(), csvClusterCostFilename);
    exportCsv(
        csv0,
        result.costTimeSeries().entrySet().stream()
            .sorted(Map.Entry.comparingByKey(Comparator.comparingLong(x -> x)))
            .map(e -> List.of(e.getKey(), e.getValue().value())));

    var csvVerboseFilename = "cost-profiling-" + time + "-" + randomName + "-verbose.csv";
    var csv1 = Path.of(arg.exportFolder.toAbsolutePath().toString(), csvVerboseFilename);
    exportCsv(
        csv1,
        Stream.concat(
                result.costTimeSeries().entrySet().stream()
                    // time, cluster-cost-value, move-cost-overflow, cluster-cost, move-cost
                    .map(e -> List.of(e.getKey(), e.getValue().value(), "", e.getValue(), "")),
                result.moveCostTimeSeries().entrySet().stream()
                    // time, cluster-cost-value, move-cost-overflow, cluster-cost, move-cost
                    .map(
                        e ->
                            List.of(
                                e.getKey(), "", e.getValue().overflow() ? 1 : 0, "", e.getValue())))
            .sorted(Comparator.comparingLong(x -> (long) x.get(0))));

    return String.format(
        format,
        // summary
        result.initial().value(),
        result.initial(),
        result
            .plan()
            .map(Balancer.Plan::proposalClusterCost)
            .map(ClusterCost::value)
            .map(Object::toString)
            .orElse("no solution found"),
        result
            .plan()
            .map(Balancer.Plan::proposalClusterCost)
            .map(Object::toString)
            .orElse("no solution found"),
        // runtime statistics
        result.executionTime(),
        result.executionTime().dividedBy(iterations).toNanos() / 1e6,
        result
                .executionTime()
                .minusNanos(result.clusterCostProcessingTimeNs().getSum())
                .minusNanos(result.moveCostProcessingTimeNs().getSum())
                .dividedBy(iterations)
                .toNanos()
            / 1e6,
        result.clusterCostProcessingTimeNs().getAverage() / 1e6,
        result.moveCostProcessingTimeNs().getAverage() / 1e6,
        result.clusterCostProcessingTimeNs().getCount(),
        result.moveCostProcessingTimeNs().getCount(),
        // details
        csv0,
        csv1);
  }

  static void exportCsv(Path location, Stream<List<Object>> timeSeries) {
    try (var writer = Utils.packException(() -> Files.newBufferedWriter(location))) {
      var iterator = timeSeries.iterator();

      while (iterator.hasNext()) {
        boolean commas = false;
        for (var item : iterator.next()) {
          if (commas) writer.write(",");
          var s = item.toString();
          // deal with commas inside the field according to RFC 4180
          if (s.contains(",")) {
            // the field should be wrapped by double-quotes
            writer.write("\"");
            // any double-quote inside the field will be covert into two double-quotes
            writer.write(s.replace("\"", "\"\""));
            // the field should be wrapped by double-quotes
            writer.write("\"");
          } else {
            writer.write(s);
          }
          commas = true;
        }
        writer.newLine();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void main(String[] args) {
    new BalancerBenchmarkApp().execute(args);
  }

  static class CommonArgument {
    @Parameter(
        names = {"--cluster.info"},
        description = "String: path to the serialized cluster info file",
        required = true,
        converter = PathField.class)
    Path serializedClusterInfo;

    @Parameter(
        names = {"--cluster.bean"},
        description = "String: path to the serialized cluster bean file",
        required = true,
        converter = PathField.class)
    Path serializedClusterBean;

    @Parameter(
        names = {"--optimization.config"},
        description =
            "String: path to the json file containing the optimization problem definition.",
        required = true,
        converter = PathField.class)
    Path optimizationConfig;

    ClusterInfo fetchClusterInfo() {
      try (var reader = Files.newInputStream(serializedClusterInfo)) {
        throw new UnsupportedOperationException();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    ClusterBean fetchClusterBean() {
      try (var reader = Files.newInputStream(serializedClusterBean)) {
        throw new UnsupportedOperationException();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    String fetchBalancerProblemJson() {
      var bytes = Utils.packException(() -> Files.readAllBytes(optimizationConfig));
      return new String(bytes);
    }

    BalancerProblemFormat fetchBalancerProblem() {
      return JsonConverter.defaultConverter()
          .fromJson(fetchBalancerProblemJson(), TypeRef.of(BalancerProblemFormat.class));
    }
  }

  static class ExperimentArgument extends CommonArgument {
    @Parameter(
        names = {"--trials"},
        description = "Integer: the number of experiments to perform.",
        required = true,
        converter = PositiveIntegerField.class)
    int trials;
  }

  static class CostProfilingArgument extends CommonArgument {
    @Parameter(
        names = {"--export.folder"},
        description = "String: the directory to store experiment result.",
        converter = PathField.class)
    Path exportFolder = Path.of(System.getProperty("java.io.tmpdir"));
  }
}
