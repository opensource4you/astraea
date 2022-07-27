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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.Utils;

/** Print out the given metrics. */
public interface TrackerThread extends AbstractThread {

  static TrackerThread create(
      List<Report> producerReports, List<Report> consumerReports, ExeTime exeTime) {
    var start = System.currentTimeMillis() - Duration.ofSeconds(1).toMillis();

    Function<Result, Boolean> logProducers =
        result -> {
          if (result.completedRecords == 0) return false;

          var duration = Duration.ofMillis(System.currentTimeMillis() - start);
          // Print completion rate (by number of records) or (by time)
          var percentage =
              Math.min(100D, exeTime.percentage(result.completedRecords, duration.toMillis()));

          System.out.println(
              "Time: "
                  + duration.toHoursPart()
                  + "hr "
                  + duration.toMinutesPart()
                  + "min "
                  + duration.toSecondsPart()
                  + "sec");
          System.out.printf("producers completion rate: %.2f%%%n", percentage);
          System.out.printf(
              "  average throughput: %.3f MB/second%n", result.averageBytes(duration));
          System.out.printf(
              "  current throughput: %s/second%n", DataSize.Byte.of(result.totalCurrentBytes()));
          System.out.println("  publish max latency: " + result.maxLatency + " ms");
          System.out.println("  publish mim latency: " + result.minLatency + " ms");
          for (int i = 0; i < result.bytes.size(); ++i) {
            System.out.printf(
                "  producer[%d] average throughput: %.3f MB%n",
                i, avg(duration, result.bytes.get(i)));
            System.out.printf(
                "  producer[%d] average publish latency: %.3f ms%n",
                i, result.averageLatencies.get(i));
          }
          System.out.println("\n");
          return percentage >= 100;
        };

    Function<Result, Boolean> logConsumers =
        result -> {
          // there is no consumer, so we just complete this log.
          if (consumerReports.isEmpty()) return true;

          if (result.completedRecords == 0) return false;

          var duration = Duration.ofMillis(System.currentTimeMillis() - start);

          // Print out percentage of (consumed records) and (produced records)
          var producerOffset =
              Report.maxOffsets(producerReports).values().stream().mapToLong(v -> v).sum();
          var consumerOffset =
              Report.maxOffsets(consumerReports).values().stream().mapToLong(v -> v).sum();
          var percentage = 100 * (double) consumerOffset / producerOffset;
          System.out.printf("consumer completion rate: %.2f%%%n", percentage);
          System.out.printf(
              "  average throughput: %.3f MB/second%n", result.averageBytes(duration));
          System.out.printf(
              "  current throughput: %s/second%n", DataSize.Byte.of(result.totalCurrentBytes()));
          System.out.println("  end-to-end max latency: " + result.maxLatency + " ms");
          System.out.println("  end-to-end mim latency: " + result.minLatency + " ms");
          for (int i = 0; i < result.bytes.size(); ++i) {
            System.out.printf(
                "  consumer[%d] average throughput: %.3f MB%n",
                i, avg(duration, result.bytes.get(i)));
            System.out.printf(
                "  consumer[%d] average ene-to-end latency: %.3f ms%n",
                i, result.averageLatencies.get(i));
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
            while (!closed.get()) {
              var producerResult = result(producerReports);
              var consumerResult = result(consumerReports);
              if (logProducers.apply(producerResult) & logConsumers.apply(consumerResult)) return;

              // Log after waiting for one second
              Utils.sleep(Duration.ofSeconds(1));
            }
          } finally {
            latch.countDown();
          }
        });

    return new TrackerThread() {

      @Override
      public Result producerResult() {
        return result(producerReports);
      }

      @Override
      public Result consumerResult() {
        return result(consumerReports);
      }

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
      }
    };
  }

  static double avg(Duration duration, long value) {
    return duration.toSeconds() <= 0
        ? 0
        : ((double) (value / duration.toSeconds())) / 1024D / 1024D;
  }

  private static Result result(List<Report> metrics) {
    var completed = 0;
    var bytes = new ArrayList<Long>();
    var currentBytes = new ArrayList<Long>();
    var averageLatencies = new ArrayList<Double>();
    var max = 0L;
    var min = Long.MAX_VALUE;
    for (var data : metrics) {
      completed += data.records();
      bytes.add(data.totalBytes());
      currentBytes.add(data.clearAndGetCurrentBytes());
      averageLatencies.add(data.avgLatency());
      max = Math.max(max, data.max());
      min = Math.min(min, data.min());
    }
    return new Result(
        completed,
        Collections.unmodifiableList(bytes),
        Collections.unmodifiableList(currentBytes),
        Collections.unmodifiableList(averageLatencies),
        min,
        max);
  }

  class Result {
    public final long completedRecords;
    public final List<Long> bytes;
    public final List<Long> currentBytes;
    public final List<Double> averageLatencies;
    public final long minLatency;
    public final long maxLatency;

    Result(
        long completedRecords,
        List<Long> bytes,
        List<Long> currentBytes,
        List<Double> averageLatencies,
        long minLatency,
        long maxLatency) {
      this.completedRecords = completedRecords;
      this.bytes = bytes;
      this.currentBytes = currentBytes;
      this.averageLatencies = averageLatencies;
      this.minLatency = minLatency;
      this.maxLatency = maxLatency;
    }

    double averageBytes(Duration duration) {
      return avg(duration, totalBytes());
    }

    long totalBytes() {
      return bytes.stream().mapToLong(i -> i).sum();
    }

    long totalCurrentBytes() {
      return currentBytes.stream().mapToLong(i -> i).sum();
    }
  }

  Result producerResult();

  Result consumerResult();

  long startTime();
}
