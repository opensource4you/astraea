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
import java.util.List;
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
      List<Report> producerReports,
      List<Report> consumerReports)
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
        initCSVFormat(writer, producerReports.size(), consumerReports.size());
        return () -> {
          try {
            while (true) {
              if (logToCSV(
                  writer,
                  exeTime,
                  consumerDone,
                  producerDone,
                  producedRecords,
                  producerReports,
                  consumerReports)) return;
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
              if (logToJSON(
                  writer,
                  exeTime,
                  consumerDone,
                  producerDone,
                  producedRecords,
                  producerReports,
                  consumerReports)) return;
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
      List<Report> producerReports,
      List<Report> consumerReports) {
    var result = processResult(exeTime, producerReports, consumerReports, producedRecords);
    if (producerReports.stream().mapToLong(Report::records).sum() == 0) return false;
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
              + producerReports.stream().mapToLong(Report::max).max().orElse(0)
              + ","
              + producerReports.stream().mapToLong(Report::min).min().orElse(0));
      writer.write(
          ","
              + consumerReports.stream().mapToLong(Report::max).max().orElse(0)
              + ","
              + consumerReports.stream().mapToLong(Report::min).min().orElse(0));
      for (var r : producerReports) {
        writer.write(
            "," + DataSize.Byte.of(r.totalBytes()).measurement(DataUnit.MiB).doubleValue());
        writer.write("," + r.avgLatency());
      }
      for (var r : consumerReports) {
        writer.write(
            "," + DataSize.Byte.of((long) r.totalBytes()).measurement(DataUnit.MiB).doubleValue());
        writer.write("," + r.avgLatency());
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
      List<Report> producerReports,
      List<Report> consumerReports) {
    var result = processResult(exeTime, producerReports, consumerReports, producedRecords);
    if (producerReports.stream().mapToLong(Report::records).sum() == 0) return false;
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
          ", \"outputThroughput\": "
              + Utils.averageMB(
                  result.duration, producerReports.stream().mapToLong(Report::totalBytes).sum()));
      writer.write(
          ", \"inputThroughput\": "
              + Utils.averageMB(
                  result.duration, consumerReports.stream().mapToLong(Report::totalBytes).sum()));
      writer.write(
          ", \"publishMaxLatency\": "
              + producerReports.stream().mapToLong(Report::max).max().orElse(0));
      writer.write(
          ", \"publishMinLatency\": "
              + producerReports.stream().mapToLong(Report::min).min().orElse(0));
      writer.write(
          ", \"E2EMaxLatency\": "
              + consumerReports.stream().mapToLong(Report::max).max().orElse(0));
      writer.write(
          ", \"E2EMinLatency\": "
              + consumerReports.stream().mapToLong(Report::min).min().orElse(0));

      writer.write(", \"producerThroughput\": [");
      for (var r : producerReports)
        writer.write(Utils.averageMB(result.duration, r.totalBytes()) + ", ");
      writer.write("], \"producerLatency\": [");
      for (var r : producerReports)
        writer.write(Utils.averageMB(result.duration, (long) r.avgLatency()) + ", ");
      writer.write("], \"consumerThroughput\": [");
      for (var r : consumerReports)
        writer.write(Utils.averageMB(result.duration, r.totalBytes()) + ", ");
      writer.write("], \"consumerLatency\": [");
      for (var r : consumerReports)
        writer.write(Utils.averageMB(result.duration, (long) r.avgLatency()) + ", ");
      writer.write("]}");
      writer.newLine();
    } catch (IOException ignore) {
    }
    return producerDone.get() && consumerDone.get();
  }

  private static ProcessedResult processResult(
      ExeTime exeTime,
      List<Report> producerReports,
      List<Report> consumerReports,
      Supplier<Long> producedRecords) {
    var duration = Duration.ofMillis(System.currentTimeMillis() - System.currentTimeMillis());
    return new ProcessedResult(
        duration,
        Math.min(
            100D,
            exeTime.percentage(
                producerReports.stream().mapToLong(Report::records).sum(), duration.toMillis())),
        consumerReports.stream().mapToLong(Report::records).sum() * 100D / producedRecords.get());
  }

  static class ProcessedResult {
    public final Duration duration;
    public final double consumerPercentage;
    public final double producerPercentage;

    ProcessedResult(Duration duration, double consumerPercentage, double producerPercentage) {
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
