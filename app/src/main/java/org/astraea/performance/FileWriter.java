package org.astraea.performance;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.IntStream;
import org.astraea.concurrent.ThreadPool;

public class FileWriter implements ThreadPool.Executor {
  private final Manager manager;
  private final Tracker tracker;
  private BufferedWriter writer;
  private String CSVName;

  public FileWriter(String path, Manager manager, Tracker tracker) throws IOException {
    this.manager = manager;
    this.tracker = tracker;
    initFileWriter(path);
  }

  @Override
  public ThreadPool.Executor.State execute() throws InterruptedException {
    if (logToCSV()) return State.DONE;
    Thread.sleep(1000);
    return State.RUNNING;
  }

  public String CSVName() {
    return CSVName;
  }

  private void initFileWriter(String path) throws IOException {
    CSVName = "Performance" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + ".csv";

    // check whether to use customized filename
    if (path.endsWith(".csv")) CSVName = path;
    else if (path.endsWith("/")) CSVName = path + CSVName;
    else CSVName = path + "/" + CSVName;
    writer = new BufferedWriter(new java.io.FileWriter(CSVName));

    writer.write(
        "Time \\ Name, Consumed/Produced, Output throughput (MiB/sec), Input throughput (MiB/sec), "
            + "Publish max latency (ms), Publish min latency (ms), "
            + "End-to-end max latency (ms), End-to-end min latency (ms)");
    IntStream.range(0, tracker.producerResult().bytes.size())
        .forEach(
            i -> {
              try {
                writer.write(
                    ",Producer["
                        + i
                        + "] average throughput (MB/sec), Producer["
                        + i
                        + "] average publish latency (ms)");
              } catch (IOException ignore) {
              }
            });
    IntStream.range(0, tracker.consumerResult().bytes.size())
        .forEach(
            i -> {
              try {
                writer.write(
                    ",Consumer["
                        + i
                        + "] average throughput (MB/sec), Consumer["
                        + i
                        + "] average ene-to-end latency (ms)");
              } catch (IOException ignore) {
              }
            });
    writer.newLine();
  }

  private boolean logToCSV() {
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
      writer.write("," + producerResult.averageBytes(duration));
      writer.write("," + consumerResult.averageBytes(duration));
      writer.write("," + producerResult.maxLatency + "," + producerResult.minLatency);
      writer.write("," + consumerResult.maxLatency + "," + consumerResult.minLatency);
      for (int i = 0; i < producerResult.bytes.size(); ++i) {
        writer.write("," + Tracker.avg(duration, producerResult.bytes.get(i)));
        writer.write("," + producerResult.averageLatencies.get(i));
      }
      for (int i = 0; i < consumerResult.bytes.size(); ++i) {
        writer.write("," + Tracker.avg(duration, consumerResult.bytes.get(i)));
        writer.write("," + consumerResult.averageLatencies.get(i));
      }
      writer.newLine();
    } catch (IOException ignore) {
    }
    return manager.producedDone() && manager.consumedDone();
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException ignore) {
    }
  }
}
