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
package org.astraea.app.balancer.simulation;

import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.util.Pair;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.simulation.events.ClusterEvent;
import org.astraea.app.balancer.simulation.events.NewProducerEvent;
import org.astraea.app.balancer.simulation.events.StopProducerEvent;
import org.astraea.app.balancer.simulation.events.TopicCreationEvent;
import org.astraea.app.balancer.simulation.events.TopicDeletionEvent;
import org.astraea.app.common.DataRate;
import org.astraea.app.common.Utils;

public class ImbalanceSimulator {

  private final Argument argument;

  public ImbalanceSimulator(Argument argument) {
    this.argument = argument;
  }

  public void execute() {
    // create some workload process
    List<ClusterProcess> processes = List.of(adhocWorkload());

    // observe these workload processes
    int simulatedHours = 24 * 30;
    System.out.println("[Cluster event generation]");
    var observedEvents =
        IntStream.range(0, simulatedHours)
            .mapToObj(ignore -> processes)
            .flatMap(Collection::stream)
            .map(process -> process.observe(TimeUnit.HOURS.toMillis(1)))
            .flatMap(Collection::stream)
            .filter(event -> event.eventTime() < TimeUnit.HOURS.toMillis(simulatedHours))
            .sorted(Comparator.comparingLong(ClusterEvent::eventTime))
            .collect(Collectors.toUnmodifiableList());
    System.out.println(" total events: " + observedEvents.size());
    System.out.println();

    System.out.println("[Simulation]");
    try (Admin admin = Admin.of(argument.configs())) {
      var simulation = ClusterSimulation.pseudo(admin);
      System.out.println(" running...");
      observedEvents.forEach(simulation::execute);
      System.out.println(" done...");
      System.out.println();

      publishSnapshot(simulation.snapshot());
    }
  }

  private void publishSnapshot(SimulationSnapshot snapshot) {
    // prepare file
    var summaryPath =
        Utils.packException(
            () ->
                argument.resultFile == null
                    ? Files.createTempFile("imbalance-simulation-", ".summary")
                    : Files.createFile(argument.resultFile));
    var jsonPath =
        Utils.packException(
            () ->
                Files.createFile(
                    Path.of(
                        summaryPath
                            .toAbsolutePath()
                            .toString()
                            .replaceFirst(".summary$", ".json"))));
    var producerPath =
        Utils.packException(
            () ->
                Files.createFile(
                    Path.of(
                        summaryPath
                            .toAbsolutePath()
                            .toString()
                            .replaceFirst(".summary$", ".producer.json"))));

    // output result to files
    try (var summaryWriter = new PrintWriter(summaryPath.toString());
        var jsonWriter = new PrintWriter(jsonPath.toString());
        var producerWriter = new PrintWriter(producerPath.toString())) {
      snapshot.summary().writeSummary(summaryWriter);
      new Gson().toJson(snapshot.logs(), jsonWriter);
      new GsonBuilder()
          .registerTypeAdapter(
              NewProducerEvent.SimulatedProducerImpl.class, new NewProducerEvent.Serialize())
          .create()
          .toJson(snapshot.producers(), producerWriter);

      summaryWriter.println("[Related files]");
      summaryWriter.println(" * this summary: " + summaryPath);
      summaryWriter.println(" * log allocation: " + jsonPath);
      summaryWriter.println(" * producer info: " + producerPath);
      summaryWriter.println();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // output summary
    Utils.packException(() -> Files.readAllLines(summaryPath).forEach(System.out::println));
  }

  public static void main(String[] args) {
    new ImbalanceSimulator(org.astraea.app.argument.Argument.parse(new Argument(), args)).execute();
  }

  public static class Argument extends org.astraea.app.argument.Argument {

    @Parameter(
        names = {"--result.path"},
        description =
            "Path to store the result file, some suffix will be added to identify file identity."
                + " If leave unspecified, a temporary file will be generate and store the result.")
    Path resultFile;
  }

  private Set<TopicPartition> produceTargets(String topic, int maxPartitions, int select) {
    var candidates = IntStream.range(0, maxPartitions).boxed().collect(Collectors.toList());
    Collections.shuffle(candidates);
    return candidates.stream()
        .limit(select)
        .map(p -> TopicPartition.of(topic, p))
        .collect(Collectors.toUnmodifiableSet());
  }

  private ClusterProcess adhocWorkload() {
    final var topicCreationPerDay = 5.0;
    final var topicNameSupplier = (Supplier<String>) () -> "Adhoc-" + Utils.randomString();
    final var topicLifeTimeAverage = TimeUnit.DAYS.toMillis(3);

    final var maxProducerEgress = DataRate.MiB.of(80).perSecond();
    final var minProducerEgress = DataRate.KiB.of(1).perSecond();
    final var randomGenerator = new Well19937c();

    final var time = new AtomicLong();
    return ms -> {
      final var topicCreationDistribution =
          new PoissonDistribution(topicCreationPerDay / TimeUnit.DAYS.toMillis(1) * ms);
      final var topicLifeTimeDistribution = new ExponentialDistribution(topicLifeTimeAverage);
      final var topicPartitionSizeDistribution = new UniformIntegerDistribution(3, 20);
      final var topicReplicaFactorDistribution =
          new EnumeratedDistribution<>(
              List.of(
                  Pair.create((short) 1, 0.7),
                  Pair.create((short) 2, 0.2),
                  Pair.create((short) 3, 0.1)));
      final var producerPerTopic = new UniformIntegerDistribution(1, 4);
      final var producerRate =
          new UniformRealDistribution(minProducerEgress.byteRate(), maxProducerEgress.byteRate());
      // create topics
      final var createTopics =
          IntStream.range(0, topicCreationDistribution.sample())
              .mapToObj(
                  ignore ->
                      TopicCreationEvent.of(
                          time.get(),
                          topicNameSupplier.get(),
                          topicPartitionSizeDistribution.sample(),
                          topicReplicaFactorDistribution.sample()))
              .collect(Collectors.toUnmodifiableList());

      // delete topics
      final var deleteTopics =
          createTopics.stream()
              .map(
                  create ->
                      TopicDeletionEvent.of(
                          time.get() + (long) topicLifeTimeDistribution.sample(),
                          create.topicName()))
              .collect(Collectors.toUnmodifiableList());

      // new producers
      final var producers =
          IntStream.range(0, producerPerTopic.sample())
              .boxed()
              .flatMap(ignore -> createTopics.stream())
              .map(
                  topic ->
                      NewProducerEvent.of(
                          time.get(),
                          Utils.randomString(),
                          produceTargets(
                              topic.topicName(),
                              topic.partitionSize(),
                              1 + randomGenerator.nextInt(topic.partitionSize() - 1)),
                          DataRate.Byte.of((long) producerRate.sample()).perSecond()))
              .collect(Collectors.toUnmodifiableList());

      // remove producers
      final var removeProducer =
          producers.stream()
              .map(
                  producer ->
                      StopProducerEvent.of(
                          deleteTopics.stream()
                              .filter(
                                  x ->
                                      x.topicName()
                                          .equals(
                                              producer.targets().stream()
                                                  .findAny()
                                                  .orElseThrow()
                                                  .topic()))
                              .findFirst()
                              .orElseThrow()
                              .eventTime(),
                          producer.producerId()))
              .collect(Collectors.toUnmodifiableList());

      time.addAndGet(ms);
      return Stream.<List<? extends ClusterEvent>>of(
              createTopics, deleteTopics, producers, removeProducer)
          .flatMap(Collection::stream)
          .collect(Collectors.toUnmodifiableList());
    };
  }
}
