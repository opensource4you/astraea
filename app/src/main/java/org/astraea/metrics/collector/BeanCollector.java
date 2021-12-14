package org.astraea.metrics.collector;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.astraea.Utils;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.MBeanClient;

public class BeanCollector {

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private BiFunction<String, Integer, MBeanClient> clientCreator = MBeanClient::jndi;
    private Duration interval = Duration.ofSeconds(3);
    private int numberOfObjectsPerNode = 300;

    private Builder() {}

    public Builder clientCreator(BiFunction<String, Integer, MBeanClient> clientCreator) {
      this.clientCreator = Objects.requireNonNull(clientCreator);
      return this;
    }

    public Builder interval(Duration interval) {
      this.interval = Objects.requireNonNull(interval);
      return this;
    }

    public Builder numberOfObjectsPerNode(int numberOfObjectsPerNode) {
      this.numberOfObjectsPerNode = Utils.requirePositive(numberOfObjectsPerNode);
      return this;
    }

    public BeanCollector build() {
      return new BeanCollector(clientCreator, interval, numberOfObjectsPerNode);
    }
  }

  private final BiFunction<String, Integer, MBeanClient> clientCreator;
  private final Duration interval;
  private final int numberOfObjectsPerNode;

  // visible for testing
  final ConcurrentMap<Node, NodeClient> clients = new ConcurrentSkipListMap<>();

  private BeanCollector(
      BiFunction<String, Integer, MBeanClient> clientCreator,
      Duration interval,
      int numberOfObjectsPerNode) {
    this.clientCreator = clientCreator;
    this.interval = interval;
    this.numberOfObjectsPerNode = numberOfObjectsPerNode;
  }

  public Register register() {
    return new Register() {
      private String host;
      private int port = -1;
      private Function<MBeanClient, HasBeanObject> getter;

      @Override
      public Register host(String host) {
        this.host = Objects.requireNonNull(host);
        return this;
      }

      @Override
      public Register port(int port) {
        this.port = Utils.requirePositive(port);
        return this;
      }

      @Override
      public Register metricsGetter(Function<MBeanClient, HasBeanObject> getter) {
        this.getter = Objects.requireNonNull(getter);
        return this;
      }

      @Override
      public Receiver build() {
        var node = new Node(host, port);
        var client = clients.computeIfAbsent(node, ignored -> new NodeClient());
        return client.add(
            new Receiver() {
              @Override
              public String host() {
                return host;
              }

              @Override
              public int port() {
                return port;
              }

              @Override
              public List<HasBeanObject> current() {
                client.tryUpdate(
                    interval,
                    numberOfObjectsPerNode,
                    getter,
                    () -> clientCreator.apply(host, port));
                return client.current();
              }

              @Override
              public void close() {
                client.remove(this);
              }
            });
      }
    };
  }

  // visible for testing
  static class NodeClient {
    private final Set<Receiver> receivers = new HashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // visible for testing
    MBeanClient mBeanClient;
    private final Map<Long, HasBeanObject> objects = new HashMap<>();

    Receiver add(Receiver receiver) {
      lock.writeLock().lock();
      try {
        receivers.add(receiver);
      } finally {
        lock.writeLock().unlock();
      }
      return receiver;
    }

    void remove(Receiver receiver) {
      lock.writeLock().lock();
      try {
        receivers.remove(receiver);
        if (receivers.isEmpty()) Utils.close(mBeanClient);
        mBeanClient = null;
      } finally {
        lock.writeLock().unlock();
      }
    }

    private void removeOldest() {
      objects.keySet().stream().min((Long::compare)).ifPresent(objects::remove);
    }

    List<HasBeanObject> current() {
      lock.readLock().lock();
      try {
        return List.copyOf(objects.values());
      } finally {
        lock.readLock().unlock();
      }
    }

    void tryUpdate(
        Duration interval,
        int numberOfObjectsPerNode,
        Function<MBeanClient, HasBeanObject> beanGetter,
        Supplier<MBeanClient> clientCreator) {

      if (lock.writeLock().tryLock()) {
        try {
          var needUpdate =
              objects.keySet().stream()
                  .max((Long::compare))
                  .map(last -> last + interval.toMillis() <= System.currentTimeMillis())
                  .orElse(true);
          if (needUpdate) {
            if (mBeanClient == null) mBeanClient = clientCreator.get();
            if (objects.size() >= numberOfObjectsPerNode) removeOldest();
            objects.put(System.currentTimeMillis(), beanGetter.apply(mBeanClient));
          }

        } finally {
          lock.writeLock().unlock();
        }
      }
    }
  }

  private static final class Node implements Comparable<Node> {
    public final String host;
    public final int port;

    Node(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public int compareTo(Node other) {
      var result = host.compareTo(other.host);
      if (result != 0) return result;
      return Integer.compare(port, other.port);
    }
  }
}
