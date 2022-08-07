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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;

public enum ReportFormat {
  CSV("csv"),
  JSON("json");

  private final String name;

  ReportFormat(String name) {
    this.name = name;
  }

  public static class ReportFormatConverter implements IStringConverter<ReportFormat> {
    @Override
    public ReportFormat convert(String value) {
      switch (value.toLowerCase()) {
        case "csv":
          return ReportFormat.CSV;
        case "json":
          return ReportFormat.JSON;
        default:
          throw new ParameterException("Invalid file format. Use \"csv\" or \"json\"");
      }
    }
  }

  public static Runnable createFileWriter(
      ReportFormat reportFormat,
      Path path,
      ExeTime exeTime,
      Supplier<Boolean> consumerDone,
      Supplier<Boolean> producerDone,
      Supplier<Long> producedRecords,
      TrackerThread tracker)
      throws IOException {
    var filePath =
        FileSystems.getDefault()
            .getPath(
                path.toString(),
                "Performance"
                    + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                    + "."
                    + reportFormat);
    var writer = new BufferedWriter(new FileWriter(filePath.toFile()));
    switch (reportFormat) {
      case CSV:
        initCSVFormat(
            writer, tracker.producerResult().bytes.size(), tracker.consumerResult().bytes.size());
        return () -> {
          try {
            while (true) {
              if (logToCSV(writer, exeTime, consumerDone, producerDone, producedRecords, tracker))
                return;
              Utils.sleep(Duration.ofSeconds(1));
            }
          } finally {
            Utils.packException(writer::close);
          }
        };
      case JSON:
        writer.write("{");
        return () -> {
          try {
            while (true) {
              if (logToJSON(writer, exeTime, consumerDone, producerDone, producedRecords, tracker))
                return;
              Utils.sleep(Duration.ofSeconds(1));
            }
          } finally {
            Utils.packException(writer::close);
          }
        };
      default:
        throw new IllegalArgumentException("Invalid format.");
    }
  }

  private static void initCSVFormat(BufferedWriter writer, int producerCounts, int consumerCounts)
      throws IOException {
    writer.write(
        "Time \\ Name, Consumed/Produced, Output throughput (MiB/sec), Input throughput (MiB/sec), "
            + "Publish max latency (ms), Publish min latency (ms), "
            + "End-to-end max latency (ms), End-to-end min latency (ms)");
    IntStream.range(0, producerCounts)
        .forEach(
            i -> {
              try {
                writer.write(
                    ",Producer["
                        + i
                        + "] current throughput (MiB/sec), Producer["
                        + i
                        + "] average publish latency (ms)");
              } catch (IOException ignore) {
              }
            });
    IntStream.range(0, consumerCounts)
        .forEach(
            i -> {
              try {
                writer.write(
                    ",Consumer["
                        + i
                        + "] current throughput (MiB/sec), Consumer["
                        + i
                        + "] average ene-to-end latency (ms)");
              } catch (IOException ignore) {
              }
            });
    writer.newLine();
  }

  private static boolean logToCSV(
      BufferedWriter writer,
      ExeTime exeTime,
      Supplier<Boolean> consumerDone,
      Supplier<Boolean> producerDone,
      Supplier<Long> producedRecords,
      TrackerThread tracker) {
    var result = processResult(exeTime, tracker, producedRecords);
    if (result.producerResult.completedRecords == 0) return false;
    try {
      writer.write(
          result.duration.toHoursPart()
              + ":"
              + result.duration.toMinutesPart()
              + ":"
              + result.duration.toSecondsPart());
      writer.write(
          String.format(",%.2f%% / %.2f%%", result.consumerPercentage, result.producerPercentage));
      writer.write(
          ","
              + DataSize.Byte.of(result.producerResult.totalCurrentBytes())
                  .measurement(DataUnit.MiB)
                  .doubleValue());
      writer.write(
          ","
              + DataSize.Byte.of(result.consumerResult.totalCurrentBytes())
                  .measurement(DataUnit.MiB)
                  .doubleValue());
      writer.write("," + result.producerResult.maxLatency + "," + result.producerResult.minLatency);
      writer.write("," + result.consumerResult.maxLatency + "," + result.consumerResult.minLatency);
      for (int i = 0; i < result.producerResult.bytes.size(); ++i) {
        writer.write(
            ","
                + DataSize.Byte.of(result.producerResult.currentBytes.get(i))
                    .measurement(DataUnit.MiB)
                    .doubleValue());
        writer.write("," + result.producerResult.averageLatencies.get(i));
      }
      for (int i = 0; i < result.consumerResult.bytes.size(); ++i) {
        writer.write(
            ","
                + DataSize.Byte.of(result.consumerResult.currentBytes.get(i))
                    .measurement(DataUnit.MiB)
                    .doubleValue());
        writer.write("," + result.consumerResult.averageLatencies.get(i));
      }
      writer.newLine();
    } catch (IOException ignore) {
    }
    return producerDone.get() && consumerDone.get();
  }

  /** Write to writer. Output: "(timestamp)": { (many metrics ...) } */
  private static boolean logToJSON(
      BufferedWriter writer,
      ExeTime exeTime,
      Supplier<Boolean> consumerDone,
      Supplier<Boolean> producerDone,
      Supplier<Long> producedRecords,
      TrackerThread tracker) {
    var result = processResult(exeTime, tracker, producedRecords);
    if (result.producerResult.completedRecords == 0) return false;
    try {
      writer.write(
          String.format(
              "\"%dh%dm%ds\": {",
              result.duration.toHoursPart(),
              result.duration.toMinutesPart(),
              result.duration.toSecondsPart()));
      writer.write(
          String.format(
              "\"consumerPercentage\": %.2f%%, \"Producer Percentage\": %.2f%%",
              result.consumerPercentage, result.producerPercentage));
      writer.write(
          ", \"outputThroughput\": " + result.producerResult.averageBytes(result.duration));
      writer.write(", \"inputThroughput\": " + result.consumerResult.averageBytes(result.duration));
      writer.write(", \"publishMaxLatency\": " + result.producerResult.maxLatency);
      writer.write(", \"publishMinLatency\": " + result.producerResult.minLatency);
      writer.write(", \"E2EMaxLatency\": " + result.consumerResult.maxLatency);
      writer.write(", \"E2EMinLatency\": " + result.consumerResult.minLatency);

      writer.write(", \"producerThroughput\": [");
      for (int i = 0; i < result.producerResult.bytes.size(); ++i) {
        writer.write(TrackerThread.avg(result.duration, result.producerResult.bytes.get(i)) + ", ");
      }
      writer.write("], \"producerLatency\": [");
      for (int i = 0; i < result.producerResult.bytes.size(); ++i) {
        writer.write(result.producerResult.averageLatencies.get(i) + ", ");
      }
      writer.write("], \"consumerThroughput\": [");
      for (int i = 0; i < result.consumerResult.bytes.size(); ++i) {
        writer.write(TrackerThread.avg(result.duration, result.consumerResult.bytes.get(i)) + ", ");
      }
      writer.write("], \"consumerLatency\": [");
      for (int i = 0; i < result.consumerResult.bytes.size(); ++i) {
        writer.write(result.consumerResult.averageLatencies.get(i) + ", ");
      }
      writer.write("]}");
      writer.newLine();
    } catch (IOException ignore) {
    }
    return producerDone.get() && consumerDone.get();
  }

  private static ProcessedResult processResult(
      ExeTime exeTime, TrackerThread tracker, Supplier<Long> producedRecords) {
    var producerResult = tracker.producerResult();
    var consumerResult = tracker.consumerResult();
    var duration = Duration.ofMillis(System.currentTimeMillis() - tracker.startTime());
    return new ProcessedResult(
        consumerResult,
        producerResult,
        duration,
        Math.min(100D, exeTime.percentage(producerResult.completedRecords, duration.toMillis())),
        consumerResult.completedRecords * 100D / producedRecords.get());
  }

  static class ProcessedResult {
    public final TrackerThread.Result consumerResult;
    public final TrackerThread.Result producerResult;
    public final Duration duration;
    public final double consumerPercentage;
    public final double producerPercentage;

    ProcessedResult(
        TrackerThread.Result consumerResult,
        TrackerThread.Result producerResult,
        Duration duration,
        double consumerPercentage,
        double producerPercentage) {
      this.consumerResult = consumerResult;
      this.producerResult = producerResult;
      this.duration = duration;
      this.consumerPercentage = consumerPercentage;
      this.producerPercentage = producerPercentage;
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
