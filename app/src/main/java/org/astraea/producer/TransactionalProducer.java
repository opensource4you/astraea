package org.astraea.producer;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

public interface TransactionalProducer<Key, Value> extends AutoCloseable {
  Sender<Key, Value> sender();

  Collection<CompletionStage<Metadata>> transaction(Collection<Sender<Key, Value>> senders);

  /** this method is blocked until all data in buffer are sent. */
  void flush();

  void close();
}
