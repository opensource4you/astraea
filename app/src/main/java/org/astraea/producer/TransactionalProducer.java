package org.astraea.producer;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

public interface TransactionalProducer<Key, Value> extends Producer<Key, Value> {
  Collection<CompletionStage<Metadata>> transaction(Collection<Sender<Key, Value>> senders);
}
