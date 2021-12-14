package org.astraea.partitioner.partitionerFactory;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.KafkaException;

/**
 * A DependencyClient allows users to make producers send dependency data.
 *
 * <pre>{@code
 * var dependencyClient = new DependencyClient();
 * dependencyClient.initializeDependency(props);
 * dependencyClient.beginDependency(props);
 * producer.send();
 * dependencyClient.finishDependency(props);
 * }</pre>
 */
public class DependencyClient {
  private static Map<Integer, SmoothWeightPartitioner> PartitionerForProducers = new HashMap<>();

  public synchronized void initializeDependency(Map<String, ?> props) {
    var ID = (int) props.get("producerID");
    isValidProps(ID);
    PartitionerForProducers.get(ID).initializeDependency();
  }

  public synchronized void beginDependency(Map<String, ?> props) {
    var ID = (int) props.get("producerID");
    isValidProps(ID);
    PartitionerForProducers.get(ID).beginDependency();
  }

  public synchronized void finishDependency(Map<String, ?> props) {
    var ID = (int) props.get("producerID");
    isValidProps(ID);
    PartitionerForProducers.get(ID).finishDependency();
  }

  private synchronized void isValidProps(int ID) {
    if (!PartitionerForProducers.containsKey(ID)) {
      throw new KafkaException("Properties does not exist in producers. ");
    }
  }

  public static synchronized void addPartitioner(
      Map<String, ?> props, SmoothWeightPartitioner partitioner) {
    var ID = (int) props.get("producerID");
    if (!PartitionerForProducers.containsKey(ID)) {
      PartitionerForProducers.put(ID, partitioner);
    } else {
      throw new KafkaException("Each producer ID can only correspond to one partitioner.");
    }
  }
}
