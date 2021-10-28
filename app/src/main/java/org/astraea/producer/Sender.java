package org.astraea.producer;

import java.util.concurrent.CompletionStage;

public interface Sender<Key, Value> {
  Sender<Key, Value> key(Key key);

  Sender<Key, Value> value(Value value);

  Sender<Key, Value> topic(String topic);

  Sender<Key, Value> partition(int partition);

  Sender<Key, Value> timestamp(long timestamp);

  CompletionStage<Metadata> run();
}
