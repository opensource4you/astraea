package org.astraea.producer;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.astraea.consumer.Header;

public abstract class AbstractSender<Key, Value> implements Sender<Key, Value> {
  Key key;
  Value value;
  String topic;
  Integer partition;
  Long timestamp;
  Collection<Header> headers = List.of();

  @Override
  public Sender<Key, Value> key(Key key) {
    this.key = key;
    return this;
  }

  @Override
  public Sender<Key, Value> value(Value value) {
    this.value = value;
    return this;
  }

  @Override
  public Sender<Key, Value> topic(String topic) {
    this.topic = Objects.requireNonNull(topic);
    return this;
  }

  @Override
  public Sender<Key, Value> partition(int partition) {
    if (partition >= 0) this.partition = partition;
    return this;
  }

  @Override
  public Sender<Key, Value> timestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  @Override
  public Sender<Key, Value> headers(Collection<Header> headers) {
    this.headers = headers;
    return this;
  }
}
