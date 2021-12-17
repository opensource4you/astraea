package org.astraea.metrics.collector;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
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
  final ConcurrentMap<String, Node> nodes = new ConcurrentSkipListMap<>();

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
        var nodeKey = host + ":" + port;
        var node = nodes.computeIfAbsent(nodeKey, ignored -> new Node(host, port));
        var receiver =
            new Receiver() {
              private final Map<Long, HasBeanObject> objects = new HashMap<>();

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
                tryUpdate();
                return List.copyOf(objects.values());
              }

              @Override
              public void close() {
                node.lock.lock();
                try {
                  node.receivers.remove(this);
                  if (node.receivers.isEmpty()) Utils.close(node.mBeanClient);
                  node.mBeanClient = null;
                } finally {
                  node.lock.unlock();
                }
              }

              private void tryUpdate() {
                if (node.lock.tryLock()) {
                  try {
                    var needUpdate =
                        objects.keySet().stream()
                            .max((Long::compare))
                            .map(last -> last + interval.toMillis() <= System.currentTimeMillis())
                            .orElse(true);
                    if (needUpdate) {
                      if (node.mBeanClient == null)
                        node.mBeanClient = clientCreator.apply(host, port);
                      if (objects.size() >= numberOfObjectsPerNode)
                        objects.keySet().stream().min((Long::compare)).ifPresent(objects::remove);
                      objects.put(System.currentTimeMillis(), getter.apply(node.mBeanClient));
                    }
                  } finally {
                    node.lock.unlock();
                  }
                }
              }
            };

        // add receiver
        node.lock.lock();
        try {
          node.receivers.add(receiver);
        } finally {
          node.lock.unlock();
        }
        return receiver;
      }
    };
  }

  // visible for testing
  static final class Node {
    private final Set<Receiver> receivers = new HashSet<>();
    private final Lock lock = new ReentrantLock();
    // visible for testing
    MBeanClient mBeanClient;
    public final String host;
    public final int port;

    Node(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }
}
