package org.astraea.producer;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * Difference between this and {@link org.astraea.producer.Producer} is that it is used for sending
 * records as one transaction. TransactionalProducer can be generated from {@link
 * org.astraea.producer.Builder} For example,
 *
 * <pre>{@code
 * var producer = Producer.builder().brokers("localhost:9092").buildTransactional();
 * }</pre>
 */
public interface TransactionalProducer<Key, Value> extends Producer<Key, Value> {
  Collection<CompletionStage<Metadata>> transaction(Collection<Sender<Key, Value>> senders);
}
