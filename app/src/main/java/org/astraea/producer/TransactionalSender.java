package org.astraea.producer;

import java.util.concurrent.CompletionStage;

public abstract class TransactionalSender<Key, Value> extends AbstractSender<Key, Value> {
  abstract CompletionStage<Metadata> send();
}
