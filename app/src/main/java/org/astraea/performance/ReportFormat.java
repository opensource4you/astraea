package org.astraea.performance;

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
import java.util.stream.IntStream;
import org.astraea.Utils;
import org.astraea.concurrent.Executor;
import org.astraea.concurrent.State;
import org.astraea.utils.DataUnit;

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

  public static Executor createFileWriter(
      ReportFormat reportFormat, Path path, Manager manager, Tracker tracker) throws IOException {
    var filePath =
        FileSystems.getDefault()
            .getPath(
                path.toString(),
                "Performance"
                    + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                    + reportFormat);
    var writer = new BufferedWriter(new FileWriter(filePath.toFile()));
    switch (reportFormat) {
      case CSV:
        initCSVFormat(
            writer, tracker.producerResult().bytes.size(), tracker.consumerResult().bytes.size());
        return new Executor() {
          @Override
          public State execute() throws InterruptedException {
            if (logToCSV(writer, manager, tracker)) return State.DONE;
            Thread.sleep(1000);
            return State.RUNNING;
          }

          @Override
          public void close() {
            Utils.close(writer);
          }
        };
      case JSON:
        writer.write("{");
        return new Executor() {
          @Override
          public State execute() throws InterruptedException {
            if (logToJSON(writer, manager, tracker)) return State.DONE;
            Thread.sleep(1000);
            return State.RUNNING;
          }

          @Override
          public void close() {
            try {
              writer.write("}");
              Utils.close(writer);
            } catch (IOException ignore) {
            }
          }
        };
      default:
        throw new IllegalArgumentException("Invalid format.");
    }
  }

  private static void initCSVFormat(BufferedWriter writer, int producerCounts, int consumerCounts)
      throws IOException {
    writer.write(
        "Time \\ Name, Consumed/Produced, Output throughput (/sec), Input throughput (/sec), "
            + "Publish max latency (ms), Publish min latency (ms), "
            + "End-to-end max latency (ms), End-to-end min latency (ms)");
    IntStream.range(0, producerCounts)
        .forEach(
            i -> {
              try {
                writer.write(
                    ",Producer["
                        + i
                        + "] current throughput (/sec), Producer["
                        + i
                        + "] current publish latency (ms)");
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
                        + "] current throughput (/sec), Consumer["
                        + i
                        + "] current ene-to-end latency (ms)");
              } catch (IOException ignore) {
              }
            });
    writer.newLine();
  }

  private static boolean logToCSV(BufferedWriter writer, Manager manager, Tracker tracker) {
    var result = processResult(manager, tracker);
    if (result.producerResult.completedRecords == 0) return false;
    try {
      writer.write(
          result.duration.toHoursPart()
              + "h"
              + result.duration.toMinutesPart()
              + "m"
              + result.duration.toSecondsPart()
              + "s");
      writer.write(
          String.format(",%.2f%% / %.2f%%", result.consumerPercentage, result.producerPercentage));
      writer.write("," + DataUnit.Byte.of(result.producerResult.totalCurrentBytes()));
      writer.write("," + DataUnit.Byte.of(result.consumerResult.totalCurrentBytes()));
      writer.write("," + result.producerResult.maxLatency + "," + result.producerResult.minLatency);
      writer.write("," + result.consumerResult.maxLatency + "," + result.consumerResult.minLatency);
      for (int i = 0; i < result.producerResult.bytes.size(); ++i) {
        writer.write("," + DataUnit.Byte.of(result.producerResult.currentBytes.get(i)));
        writer.write("," + result.producerResult.averageLatencies.get(i));
      }
      for (int i = 0; i < result.consumerResult.bytes.size(); ++i) {
        writer.write("," + DataUnit.Byte.of(result.consumerResult.currentBytes.get(i)));
        writer.write("," + result.consumerResult.averageLatencies.get(i));
      }
      writer.newLine();
    } catch (IOException ignore) {
    }
    return manager.producedDone() && manager.consumedDone();
  }

  /** Write to writer. Output: "(timestamp)": { (many metrics ...) } */
  private static boolean logToJSON(BufferedWriter writer, Manager manager, Tracker tracker) {
    var result = processResult(manager, tracker);
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
        writer.write(Tracker.avg(result.duration, result.producerResult.bytes.get(i)) + ", ");
      }
      writer.write("], \"producerLatency\": [");
      for (int i = 0; i < result.producerResult.bytes.size(); ++i) {
        writer.write(result.producerResult.averageLatencies.get(i) + ", ");
      }
      writer.write("], \"consumerThroughput\": [");
      for (int i = 0; i < result.consumerResult.bytes.size(); ++i) {
        writer.write(Tracker.avg(result.duration, result.consumerResult.bytes.get(i)) + ", ");
      }
      writer.write("], \"consumerLatency\": [");
      for (int i = 0; i < result.consumerResult.bytes.size(); ++i) {
        writer.write(result.consumerResult.averageLatencies.get(i) + ", ");
      }
      writer.write("]}");
      writer.newLine();
    } catch (IOException ignore) {
    }
    return manager.producedDone() && manager.consumedDone();
  }

  private static ProcessedResult processResult(Manager manager, Tracker tracker) {
    var producerResult = tracker.producerResult();
    var consumerResult = tracker.consumerResult();
    var duration = tracker.duration();
    return new ProcessedResult(
        tracker.consumerResult(),
        tracker.producerResult(),
        duration,
        Math.min(
            100D,
            manager.exeTime().percentage(producerResult.completedRecords, duration.toMillis())),
        consumerResult.completedRecords * 100D / manager.producedRecords());
  }

  static class ProcessedResult {
    public final Tracker.Result consumerResult;
    public final Tracker.Result producerResult;
    public final Duration duration;
    public final double consumerPercentage;
    public final double producerPercentage;

    ProcessedResult(
        Tracker.Result consumerResult,
        Tracker.Result producerResult,
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
