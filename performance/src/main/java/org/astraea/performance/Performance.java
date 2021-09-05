package org.astraea.performance;

public class Performance {
  public static void main(String[] args) {
    int records = 10;
    int recordSize = 1024;
    ConsumerThread consumerThread = new ConsumerThread();
    ProducerThread producerThread =
        new ProducerThread("topic", "bootstrapServers", records, recordSize);
    consumerThread.start();
    producerThread.start();
  }
}
