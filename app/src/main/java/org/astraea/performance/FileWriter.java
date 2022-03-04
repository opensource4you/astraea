package org.astraea.performance;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.IntStream;
import org.astraea.concurrent.Executor;
import org.astraea.concurrent.State;
import org.astraea.utils.DataUnit;

public class FileWriter implements Executor {
  private final Manager manager;
  private final Tracker tracker;
  private BufferedWriter writer;
  private String CSVName;

  public FileWriter(Manager manager, Tracker tracker) {
    this.manager = manager;
    this.tracker = tracker;
    initFileWriter();
  }

  @Override
  public State execute() throws InterruptedException {
    if (logToCSV()) return State.DONE;
    Thread.sleep(1000);
    return State.RUNNING;
  }

  public String CSVName() {
    return CSVName;
  }

  private void initFileWriter() {
    CSVName = "Performance" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + ".csv";
    try {
      writer = new BufferedWriter(new java.io.FileWriter(CSVName));
      writer.write(
          "Time \\ Name, Consumed/Produced, Output throughput (/sec), Input throughput (/sec), "
              + "Publish max latency (ms), Publish min latency (ms), "
              + "End-to-end max latency (ms), End-to-end min latency (ms)");
      IntStream.range(0, tracker.producerResult().bytes.size())
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
      IntStream.range(0, tracker.consumerResult().bytes.size())
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
    } catch (IOException ignore) {
      writer = null;
    }
  }

  private boolean logToCSV() {
    var producerResult = tracker.producerResult();
    var consumerResult = tracker.consumerResult();
    if (writer == null || producerResult.completedRecords == 0L) return false;
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

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException ignore) {
    }
  }
}
