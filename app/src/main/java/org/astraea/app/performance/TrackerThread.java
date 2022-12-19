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
package org.astraea.app.performance;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.Sensor;
import org.astraea.common.metrics.client.consumer.ConsumerMetrics;
import org.astraea.common.metrics.client.consumer.HasConsumerCoordinatorMetrics;
import org.astraea.common.metrics.client.producer.HasProducerTopicMetrics;
import org.astraea.common.metrics.client.producer.ProducerMetrics;
import org.astraea.common.metrics.stats.Avg;
import org.astraea.common.metrics.stats.Latest;

/** Print out the given metrics. */
public interface TrackerThread extends AbstractThread {

  class ProducerPrinter {
    private final MBeanClient mBeanClient = MBeanClient.local();
    private final Supplier<List<Report>> reportSupplier;
    private long lastRecords = 0;

    ProducerPrinter() {
      this(Report::producers);
    }

    ProducerPrinter(Supplier<List<Report>> reportSupplier) {
      this.reportSupplier = reportSupplier;
    }

    boolean tryToPrint(Duration duration) {
      var reports = reportSupplier.get();
      var records = reports.stream().mapToLong(Report::records).sum();
      System.out.println("------------[Producers]------------");
      if (records == lastRecords) {
        System.out.println("no active producers");
        return false;
      }
      lastRecords = records;
      System.out.println("  produced records: " + records);
      System.out.printf(
          "  average throughput: %s/second%n",
          DataSize.Byte.of(
              (long)
                  reports.stream()
                      .mapToDouble(Report::avgThroughput)
                      .filter(d -> !Double.isNaN(d))
                      .sum()));
      System.out.printf(
          "  error: %.1f records/second%n",
          sumOfAttribute(
              ProducerMetrics.topics(mBeanClient), HasProducerTopicMetrics::recordErrorRate));
      reports.stream()
          .mapToLong(Report::maxLatency)
          .max()
          .ifPresent(i -> System.out.printf("  publish max latency: %d ms%n", i));
      reports.stream()
          .mapToDouble(Report::avgLatency)
          .average()
          .ifPresent(i -> System.out.printf("  publish average latency: %.3f ms%n", i));
      for (int i = 0; i < reports.size(); ++i) {
        System.out.printf(
            "  producer[%d] average throughput: %s%n",
            i, DataSize.Byte.of((long) reports.get(i).avgThroughput()));
        System.out.printf(
            "  producer[%d] average publish latency: %.3f ms%n", i, reports.get(i).avgLatency());
      }
      return true;
    }
  }

  class ConsumerPrinter {
    private final MBeanClient mBeanClient = MBeanClient.local();
    private final Supplier<List<Report>> reportSupplier;
    private long lastRecords = 0;

    private static final String _15_MINUTE_AVG = "15-minute-avg";
    private static final String _5_MINUTE_AVG = "5-minute-avg";
    private static final String _1_MINUTE_AVG = "1-minute-avg";
    private final Sensor<Double> numOfPartitionSensor =
        Sensor.builder()
            .addStat(_15_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(15)))
            .addStat(_5_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(5)))
            .addStat(_1_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(1)))
            .addStat("latest", new Latest<Double>())
            .build();
    private final Sensor<Double> nonStickyPartitionSensor =
        Sensor.builder()
            .addStat(_15_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(15)))
            .addStat(_5_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(5)))
            .addStat(_1_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(1)))
            .addStat("latest", new Latest<Double>())
            .build();
    private final Sensor<Double> partitionDifferenceSensor =
        Sensor.builder()
            .addStat(_15_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(15)))
            .addStat(_5_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(5)))
            .addStat(_1_MINUTE_AVG, Avg.byTime(Duration.ofMinutes(1)))
            .addStat("latest", new Latest<Double>())
            .build();

    ConsumerPrinter() {
      this(Report::consumers);
    }

    ConsumerPrinter(Supplier<List<Report>> reportSupplier) {
      this.reportSupplier = reportSupplier;
    }

    boolean tryToPrint(Duration duration) {
      var reports = reportSupplier.get();
      var records = reports.stream().mapToLong(Report::records).sum();
      System.out.println("------------[Consumers]------------");
      if (records == lastRecords) {
        System.out.println("no active consumers");
        return false;
      }
      lastRecords = records;
      System.out.println("  consumed records: " + records);
      System.out.printf(
          "  average throughput: %s/second%n",
          DataSize.Byte.of(
              (long)
                  reports.stream()
                      .mapToDouble(Report::avgThroughput)
                      .filter(d -> !Double.isNaN(d))
                      .sum()));
      reports.stream()
          .mapToLong(Report::maxLatency)
          .max()
          .ifPresent(i -> System.out.printf("  end-to-end max latency: %d ms%n", i));
      reports.stream()
          .mapToDouble(Report::avgLatency)
          .average()
          .ifPresent(i -> System.out.printf("  end-to-end average latency: %.3f ms%n", i));
      var metrics = ConsumerMetrics.coordinators(mBeanClient);
      metrics.stream()
          .mapToDouble(HasConsumerCoordinatorMetrics::rebalanceLatencyMax)
          .max()
          .ifPresent(i -> System.out.printf("  rebalance max latency: %.3f ms%n", i));
      metrics.stream()
          .mapToDouble(HasConsumerCoordinatorMetrics::rebalanceLatencyAvg)
          .average()
          .ifPresent(i -> System.out.printf("  rebalance average latency: %.3f ms%n", i));
      for (var i = 0; i < reports.size(); ++i) {
        var report = reports.get(i);
        var clientId = report.clientId();
        var ms = metrics.stream().filter(m -> m.clientId().equals(clientId)).findFirst();

        if (ms.isPresent()) {
          numOfPartitionSensor.record(ms.get().assignedPartitions());
          nonStickyPartitionSensor.record(
              (double) ConsumerThread.nonStickyPartitionBetweenRebalance(clientId));
          partitionDifferenceSensor.record(
              (double) ConsumerThread.differenceBetweenRebalance(clientId));

          System.out.printf(
              "  consumer[%d] has %.1f partitions. "
                  + "%.1f non-sticky partitions, "
                  + "assigned %.1f more partitions than before re-balancing%n",
              i,
              numOfPartitionSensor.measure("latest"),
              nonStickyPartitionSensor.measure("latest"),
              partitionDifferenceSensor.measure("latest"));

          System.out.printf(
              "  %.2f partitions in 15 minute average, "
                  + "%.2f non-sticky partitions in 15 minute average, "
                  + "assigned %.2f more partitions than before re-balancing in 15 minute average%n",
              numOfPartitionSensor.measure(_15_MINUTE_AVG),
              nonStickyPartitionSensor.measure(_15_MINUTE_AVG),
              partitionDifferenceSensor.measure(_15_MINUTE_AVG));
        }
        System.out.printf(
            "  consumed[%d] average throughput: %s%n",
            i, DataSize.Byte.of((long) reports.get(i).avgThroughput()));
        System.out.printf(
            "  consumer[%d] average ene-to-end latency: %.3f ms%n", i, report.avgLatency());
      }
      return true;
    }
  }

  static TrackerThread create(Supplier<Boolean> producersDone, Supplier<Boolean> consumersDone) {
    var closed = new AtomicBoolean(false);
    var latch = new CountDownLatch(1);
    CompletableFuture.runAsync(trackerLoop(closed::get, producersDone, consumersDone))
        .whenComplete((m, e) -> latch.countDown());

    return new TrackerThread() {

      @Override
      public void waitForDone() {
        Utils.swallowException(latch::await);
      }

      @Override
      public boolean closed() {
        return latch.getCount() == 0;
      }

      @Override
      public void close() {
        closed.set(true);
        waitForDone();
      }
    };
  }

  static Runnable trackerLoop(
      Supplier<Boolean> closed, Supplier<Boolean> producersDone, Supplier<Boolean> consumersDone) {
    var start = System.currentTimeMillis();
    return () -> {
      var producerPrinter = new ProducerPrinter();
      var consumerPrinter = new ConsumerPrinter();
      while (!closed.get()) {
        var duration = Duration.ofMillis(System.currentTimeMillis() - start);
        System.out.println();
        System.out.println(
            "Time: "
                + duration.toHoursPart()
                + "hr "
                + duration.toMinutesPart()
                + "min "
                + duration.toSecondsPart()
                + "sec");
        producerPrinter.tryToPrint(duration);
        consumerPrinter.tryToPrint(duration);
        if (producersDone.get() && consumersDone.get()) return;
        // Log after waiting for one second
        Utils.sleep(Duration.ofSeconds(1));
      }
    };
  }

  /**
   * Sum up the latest given attribute of all beans which is instance of HasNodeMetrics.
   *
   * @param mbeans mBeans fetched by the receivers
   * @return sum of the latest given attribute of all beans which is instance of HasNodeMetrics.
   */
  static <T extends HasBeanObject> double sumOfAttribute(
      Collection<T> mbeans, ToDoubleFunction<T> targetAttribute) {
    return mbeans.stream().mapToDouble(targetAttribute).filter(d -> !Double.isNaN(d)).sum();
  }
}
