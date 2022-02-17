package org.astraea.performance;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.IntStream;
import org.astraea.concurrent.ThreadPool;
import org.astraea.utils.DataUnit;

public interface ReportWriter extends ThreadPool.Executor {

  static ReportWriter createFileWriter(
      FileFormat fileFormat, Path path, Manager manager, Tracker tracker) throws IOException {
    var filePath =
        FileSystems.getDefault()
            .getPath(
                path.toString(),
                "Performance" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + ".csv");
    var writer = new BufferedWriter(new FileWriter(filePath.toFile()));
    switch (fileFormat) {
      case CSV:
        initCSVFormat(
            writer, tracker.producerResult().bytes.size(), tracker.consumerResult().bytes.size());
        return new ReportWriter() {
          @Override
          public ThreadPool.Executor.State execute() throws InterruptedException {
            if (logToCSV(writer, manager, tracker)) return State.DONE;
            Thread.sleep(1000);
            return State.RUNNING;
          }

          @Override
          public void close() {
            try {
              writer.close();
            } catch (IOException ignore) {
            }
          }
        };
      case JSON:
        writer.write("{");
        return new ReportWriter() {
          @Override
          public ThreadPool.Executor.State execute() throws InterruptedException {
            if (logToJSON(writer, manager, tracker)) return State.DONE;
            Thread.sleep(1000);
            return State.RUNNING;
          }

          @Override
          public void close() {
            try {
              writer.write("}");
              writer.close();
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

  static boolean logToCSV(BufferedWriter writer, Manager manager, Tracker tracker) {
    var producerResult = tracker.producerResult();
    var consumerResult = tracker.consumerResult();
    if (writer == null) return true;
    if (producerResult.completedRecords == 0L) return false;
    var duration = tracker.duration();
    var producerPercentage =
        Math.min(
            100D,
            manager.exeTime().percentage(producerResult.completedRecords, duration.toMillis()));
    var consumerPercentage = consumerResult.completedRecords * 100D / manager.producedRecords();
    try {
      writer.write(
          duration.toHoursPart()
              + "h"
              + duration.toMinutesPart()
              + "m"
              + duration.toSecondsPart()
              + "s");
      writer.write(String.format(",%.2f%% / %.2f%%", consumerPercentage, producerPercentage));
      writer.write("," + DataUnit.Byte.of(producerResult.totalCurrentBytes()));
      writer.write("," + DataUnit.Byte.of(consumerResult.totalCurrentBytes()));
      writer.write("," + producerResult.maxLatency + "," + producerResult.minLatency);
      writer.write("," + consumerResult.maxLatency + "," + consumerResult.minLatency);
      for (int i = 0; i < producerResult.bytes.size(); ++i) {
        writer.write("," + DataUnit.Byte.of(producerResult.currentBytes.get(i)));
        writer.write("," + producerResult.averageLatencies.get(i));
      }
      for (int i = 0; i < consumerResult.bytes.size(); ++i) {
        writer.write("," + DataUnit.Byte.of(consumerResult.currentBytes.get(i)));
        writer.write("," + consumerResult.averageLatencies.get(i));
      }
      writer.newLine();
    } catch (IOException ignore) {
    }
    return manager.producedDone() && manager.consumedDone();
  }

  /** Write to writer. Output: "(timestamp)": { (many metrics ...) } */
  static boolean logToJSON(BufferedWriter writer, Manager manager, Tracker tracker) {
    var producerResult = tracker.producerResult();
    var consumerResult = tracker.consumerResult();
    if (writer == null) return true;
    if (producerResult.completedRecords == 0L) return false;
    var duration = tracker.duration();
    var producerPercentage =
        Math.min(
            100D,
            manager.exeTime().percentage(producerResult.completedRecords, duration.toMillis()));
    var consumerPercentage = consumerResult.completedRecords * 100D / manager.producedRecords();
    try {
      writer.write(
          String.format(
              "\"%dh%dm%ds\": {",
              duration.toHoursPart(), duration.toMinutesPart(), duration.toSecondsPart()));
      writer.write(
          String.format(
              "\"consumerPercentage\": %.2f%%, \"Producer Percentage\": %.2f%%",
              consumerPercentage, producerPercentage));
      writer.write(", \"outputThroughput\": " + producerResult.averageBytes(duration));
      writer.write(", \"inputThroughput\": " + consumerResult.averageBytes(duration));
      writer.write(", \"publishMaxLatency\": " + producerResult.maxLatency);
      writer.write(", \"publishMinLatency\": " + producerResult.minLatency);
      writer.write(", \"E2EMaxLatency\": " + consumerResult.maxLatency);
      writer.write(", \"E2EMinLatency\": " + consumerResult.minLatency);

      writer.write(", \"producerThroughput\": [");
      for (int i = 0; i < producerResult.bytes.size(); ++i) {
        writer.write(Tracker.avg(duration, producerResult.bytes.get(i)) + ", ");
      }
      writer.write("], \"producerLatency\": [");
      for (int i = 0; i < producerResult.bytes.size(); ++i) {
        writer.write(producerResult.averageLatencies.get(i) + ", ");
      }
      writer.write("], \"consumerThroughput\": [");
      for (int i = 0; i < consumerResult.bytes.size(); ++i) {
        writer.write(Tracker.avg(duration, consumerResult.bytes.get(i)) + ", ");
      }
      writer.write("], \"consumerLatency\": [");
      for (int i = 0; i < consumerResult.bytes.size(); ++i) {
        writer.write(consumerResult.averageLatencies.get(i) + ", ");
      }
      writer.write("]}");
      writer.newLine();
    } catch (IOException ignore) {
    }
    return manager.producedDone() && manager.consumedDone();
  }

  enum FileFormat {
    CSV("csv"),
    JSON("json");

    private final String name;

    FileFormat(String name) {
      this.name = name;
    }

    public static class FileFormatConverter implements IStringConverter<FileFormat> {
      @Override
      public FileFormat convert(String value) {
        switch (value.toLowerCase()) {
          case "csv":
            return FileFormat.CSV;
          case "json":
            return FileFormat.JSON;
          default:
            throw new ParameterException("Invalid file format. Use \"csv\" or \"json\"");
        }
      }
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
