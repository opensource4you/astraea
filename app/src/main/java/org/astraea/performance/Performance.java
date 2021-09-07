package org.astraea.performance;

import java.util.Properties;

public class Performance {
  public static void main(String[] args) {
    Properties prop = parseArgs(args);
    String topic = prop.getProperty("topic");
    String bootstrapServers = prop.getProperty("bootstrapServers");
    int records = Integer.parseInt(prop.getProperty("records"));
    int recordSize = Integer.parseInt(prop.getProperty("recordSize"));

    ConsumerThread consumerThread = new ConsumerThread(topic, bootstrapServers, "groupId");
    ProducerThread producerThread =
        new ProducerThread(topic, bootstrapServers, records, recordSize);

    System.out.println("Consumer starting...");
    consumerThread.start();
    System.out.println("Producer starting...");
    producerThread.start();

    while (!producerThread.end())
      ;
    consumerThread.close();

    System.out.println("Performance end.");
  }

  private static Properties parseArgs(String[] args) {
    Properties prop = new Properties();
    for (int i = 0; i < args.length; ++i) {
      if (args[i].charAt(0) == '-' && args[i].charAt(1) == '-') {
        prop.put(args[i].substring(2), args[i + 1]);
        ++i;
      }
    }
    return prop;
  }
}
