package org.astraea.partitioner.smoothPartitioner;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.KafkaException;

/**
 * A DependencyClient allows users to make producers send dependency data.
 *
 * <pre>{@code
 * var dependencyClient = new DependencyClient(props);
 * dependencyClient.initializeDependency();
 * dependencyClient.beginDependency();
 * producer.send();
 * dependencyClient.finishDependency();
 * }</pre>
 */
public class DependencyClient {
  private static final Map<Integer, SmoothWeightPartitioner> PartitionerForProducers =
      new HashMap<>();
  private final Map<String, ?> props;

  public DependencyClient(Map<String, ?> props) {
    this.props = props;
  }

  public synchronized void initializeDependency() {
    var ID = (int) props.get("producerID");
    isValidProps(ID);
    PartitionerForProducers.get(ID).initializeDependency();
  }

  public synchronized void beginDependency() {
    var ID = (int) props.get("producerID");
    isValidProps(ID);
    PartitionerForProducers.get(ID).beginDependency();
  }

  public synchronized void finishDependency() {
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
