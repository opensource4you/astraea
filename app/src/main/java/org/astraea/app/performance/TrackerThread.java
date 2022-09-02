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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.MBeanClient;
import org.astraea.app.metrics.client.HasNodeMetrics;
import org.astraea.app.metrics.client.producer.HasProducerTopicMetrics;
import org.astraea.app.metrics.client.producer.ProducerMetrics;

/** Print out the given metrics. */
public interface TrackerThread extends AbstractThread {

  static TrackerThread create(
      Supplier<List<ProducerThread.Report>> producerReporter,
      Supplier<List<ConsumerThread.Report>> consumerReporter,
      ExeTime exeTime) {
    var start = System.currentTimeMillis() - Duration.ofSeconds(1).toMillis();
    var mBeanClient = MBeanClient.local();

    Function<Duration, Boolean> logProducers =
        duration -> {
          var producerReports = producerReporter.get();
          var records = producerReports.stream().mapToLong(Report::records).sum();
          if (records == 0) return false;
          // Print completion rate (by number of records) or (by time)
          var percentage = Math.min(100D, exeTime.percentage(records, duration.toMillis()));

          System.out.printf("producers completion rate: %.2f%%%n", percentage);
          System.out.printf(
              "  average throughput: %.3f MB/second%n",
              Utils.averageMB(
                  duration, producerReports.stream().mapToLong(Report::totalBytes).sum()));
          System.out.printf(
              "  current traffic: %s/second%n",
              DataSize.Byte.of(
                  (long)
                      sumOfAttribute(
                          ProducerMetrics.nodes(mBeanClient), HasNodeMetrics::outgoingByteRate)));
          System.out.printf(
              "  error: %.1f records/second%n",
              sumOfAttribute(
                  ProducerMetrics.topics(mBeanClient), HasProducerTopicMetrics::recordErrorRate));
          producerReports.stream()
              .mapToLong(Report::max)
              .max()
              .ifPresent(i -> System.out.println("  publish max latency: " + i + " ms"));
          producerReports.stream()
              .mapToLong(Report::min)
              .min()
              .ifPresent(i -> System.out.println("  publish mim latency: " + i + " ms"));
          for (int i = 0; i < producerReports.size(); ++i) {
            System.out.printf(
                "  producer[%d] average throughput: %.3f MB%n",
                i, Utils.averageMB(duration, producerReports.get(i).totalBytes()));
            System.out.printf(
                "  producer[%d] average publish latency: %.3f ms%n",
                i, producerReports.get(i).avgLatency());
          }
          System.out.println("\n");
          return percentage >= 100;
        };

    Function<Duration, Boolean> logConsumers =
        duration -> {
          var producerReports = producerReporter.get();
          var consumerReports = consumerReporter.get();
          // there is no consumer, so we just complete this log.
          if (consumerReports.isEmpty()) return true;

          if (consumerReports.stream().mapToLong(Report::records).sum() == 0) return false;

          // Print out percentage of (consumed records) and (produced records)
          var producerOffset =
              Report.maxOffsets(producerReports).values().stream().mapToLong(v -> v).sum();
          var consumerOffset =
              Report.maxOffsets(consumerReports).values().stream().mapToLong(v -> v).sum();
          var percentage = 100 * (double) consumerOffset / producerOffset;
          System.out.printf("consumer completion rate: %.2f%%%n", percentage);
          System.out.printf(
              "  average throughput: %.3f MB/second%n",
              Utils.averageMB(
                  duration, consumerReports.stream().mapToLong(Report::totalBytes).sum()));
          System.out.printf(
              "  current traffic: %s/second%n",
              DataSize.Byte.of(
                  (long)
                      sumOfAttribute(
                          ProducerMetrics.nodes(mBeanClient), HasNodeMetrics::incomingByteRate)));
          consumerReports.stream()
              .mapToLong(Report::max)
              .max()
              .ifPresent(i -> System.out.println("  end-to-end max latency: " + i + " ms"));
          consumerReports.stream()
              .mapToLong(Report::min)
              .min()
              .ifPresent(i -> System.out.println("  end-to-end mim latency: " + i + " ms"));
          consumerReports.stream()
              .mapToLong(ConsumerThread.Report::maxSubscriptionLatency)
              .max()
              .ifPresent(i -> System.out.println("  subscription max latency: " + i + " ms"));
          consumerReports.stream()
              .mapToDouble(ConsumerThread.Report::avgSubscriptionLatency)
              .average()
              .ifPresent(i -> System.out.println("  subscription average latency: " + i + " ms"));
          for (int i = 0; i < consumerReports.size(); ++i) {
            var report = consumerReports.get(i);
            System.out.printf("  consumer[%d] has %d partitions%n", i, report.assignments().size());
            System.out.printf(
                "  consumer[%d] average throughput: %.3f MB%n",
                i, Utils.averageMB(duration, report.totalBytes()));
            System.out.printf(
                "  consumer[%d] average ene-to-end latency: %.3f ms%n", i, report.avgLatency());
          }
          System.out.println("\n");
          // Target number of records consumed OR consumed all that produced
          return percentage >= 100D;
        };

    var closed = new AtomicBoolean(false);
    var latch = new CountDownLatch(1);

    CompletableFuture.runAsync(
        () -> {
          try {
            var producerDone = false;
            var consumerDone = false;
            while (!closed.get()) {
              var duration = Duration.ofMillis(System.currentTimeMillis() - start);
              System.out.println(
                  "Time: "
                      + duration.toHoursPart()
                      + "hr "
                      + duration.toMinutesPart()
                      + "min "
                      + duration.toSecondsPart()
                      + "sec");
              if (!producerDone) producerDone = logProducers.apply(duration);
              if (!consumerDone) consumerDone = logConsumers.apply(duration);
              if (producerDone && consumerDone) return;

              // Log after waiting for one second
              Utils.sleep(Duration.ofSeconds(1));
            }
          } finally {
            latch.countDown();
          }
        });

    return new TrackerThread() {

      @Override
      public long startTime() {
        return start;
      }

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
        Utils.swallowException(mBeanClient::close);
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

  long startTime();
}
