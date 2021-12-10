package org.astraea.producer;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.astraea.consumer.Header;

public interface Sender<Key, Value> {
  Sender<Key, Value> key(Key key);

  Sender<Key, Value> value(Value value);

  Sender<Key, Value> topic(String topic);

  /**
   * define the data route if you don't want to partitioner to decide the target.
   *
   * @param partition target partition
   * @return this sender
   */
  Sender<Key, Value> partition(int partition);

  Sender<Key, Value> timestamp(long timestamp);

  Sender<Key, Value> headers(Collection<Header> headers);

  default Sender<Key, Value> transaction() {
    return this;
  }

  /**
   * send data to servers. This operation is running in background. You have to call {@link
   * CompletionStage#toCompletableFuture()} to wait response of servers.
   *
   * @return an async operation stage.
   */
  CompletionStage<Metadata> run();
}
