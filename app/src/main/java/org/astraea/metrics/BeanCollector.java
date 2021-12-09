package org.astraea.metrics;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.Utils;
import org.astraea.concurrent.ThreadPool;
import org.astraea.metrics.jmx.MBeanClient;

/**
 * this class is used to manage multiples jmx connections. Normally, we want to get different
 * metrics from the same jmx server, but we hate the high cost caused by multiples connections.
 * Hence, this class keeps the single connection for each jmx server, and you can register the
 * `getter` the fetch various mbean objects through same connection.
 */
public class BeanCollector implements AutoCloseable {

  public static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private int numberOfThreads = 2;
    private Duration interval = Duration.ofSeconds(1);
    private int numberOfObjectsPerNode = 300;

    private Builder() {}

    public Builder numberOfThreads(int numberOfThreads) {
      this.numberOfThreads = numberOfThreads;
      return this;
    }

    public Builder interval(Duration interval) {
      this.interval = interval;
      return this;
    }

    public Builder numberOfObjectsPerNode(int numberOfObjectsPerNode) {
      this.numberOfObjectsPerNode = numberOfObjectsPerNode;
      return this;
    }

    public BeanCollector build() {
      return new BeanCollector(numberOfThreads, interval, numberOfObjectsPerNode);
    }
  }

  private final Queue<NodeImpl> nodes = new ConcurrentLinkedQueue<>();
  private final ThreadPool pool;
  private final int numberOfObjectsPerNode;
  private final Object notification = new Object();

  private BeanCollector(int numberOfThreads, Duration interval, int numberOfObjectsPerNode) {
    this.numberOfObjectsPerNode = numberOfObjectsPerNode;
    this.pool =
        ThreadPool.builder()
            .executors(
                IntStream.range(0, numberOfThreads)
                    .mapToObj(
                        threadIndex ->
                            (ThreadPool.Executor)
                                () -> {
                                  nodes.forEach(NodeImpl::updateObjects);
                                  synchronized (notification) {
                                    notification.wait(interval.toMillis());
                                  }
                                  return ThreadPool.Executor.State.RUNNING;
                                })
                    .collect(Collectors.toList()))
            .build();
  }

  /** @return the monitored host/port */
  public List<Node> nodes() {
    return nodes.stream().map(n -> (Node) n).collect(Collectors.toList());
  }

  /**
   * @param host target host
   * @param port target port
   * @return the objects from target host/port
   */
  public List<HasBeanObject> objects(String host, int port) {
    return nodes.stream()
        .filter(n -> n.host().equals(host) && n.port() == port)
        .flatMap(n -> n.objects.stream())
        .collect(Collectors.toList());
  }

  public Map<Node, List<HasBeanObject>> objects() {
    return nodes.stream()
        .collect(Collectors.toMap(Function.identity(), node -> new ArrayList<>(node.objects)));
  }

  /** @return the number of all objects */
  public int numberOfObjects() {
    return nodes.stream().mapToInt(node -> node.objects.size()).sum();
  }

  /** @return the number of all getters */
  int numberOfGetters() {
    return nodes.stream()
        .mapToInt(node -> node.allGetters.values().stream().mapToInt(Deque::size).sum())
        .sum();
  }

  /** wake up all threads to update mbean objects */
  public void requestToUpdate() {
    synchronized (notification) {
      notification.notifyAll();
    }
  }

  @Override
  public void close() {
    pool.close();
    pool.waitAll();

    // close all nodes
    while (!nodes.isEmpty()) {
      var node = nodes.poll();
      if (node != null) node.close();
    }
  }

  /**
   * @return Register is used to store your getter which can fetch mbean objects from jmx
   *     connection.
   */
  public Register register() {
    return new Register() {
      private String host;
      private int port = -1;
      private Supplier<MBeanClient> supplier;
      private String getterName;
      private Function<MBeanClient, HasBeanObject> getter;

      @Override
      public Register host(String host) {
        this.host = host;
        return this;
      }

      @Override
      public Register port(int port) {
        this.port = port;
        return this;
      }

      @Override
      public Register clientSupplier(Supplier<MBeanClient> supplier) {
        this.supplier = supplier;
        return this;
      }

      @Override
      public Register metricsGetter(String name, Function<MBeanClient, HasBeanObject> getter) {
        this.getterName = name;
        this.getter = getter;
        return this;
      }

      @Override
      public Unregister build() {
        if (pool.isClosed()) throw new RuntimeException("this is closed!!!");
        var finalHost = Objects.requireNonNull(host);
        var finalPort = Utils.requirePositive(port);
        Supplier<MBeanClient> finalSupplier =
            supplier == null ? () -> MBeanClient.jndi(finalHost, finalPort) : supplier;
        var finalGetter = Objects.requireNonNull(getter);
        var finalGetterName = getterName == null ? finalGetter.toString() : getterName;
        var node =
            nodes.stream()
                .filter(n -> n.host().equals(finalHost) && n.port() == finalPort)
                .findFirst()
                .orElseGet(
                    () -> {
                      var n = new NodeImpl(finalSupplier.get(), numberOfObjectsPerNode);
                      nodes.add(n);
                      return n;
                    });
        node.allGetters
            .computeIfAbsent(finalGetterName, ignore -> new ConcurrentLinkedDeque<>())
            .add(getter);
        return () -> {
          var getters = node.allGetters.get(finalGetterName);
          if (getters != null) getters.remove(getter);
        };
      }
    };
  }

  interface Register {
    Register host(String host);

    Register port(int port);

    Register clientSupplier(Supplier<MBeanClient> supplier);

    default Register metricsGetter(Function<MBeanClient, HasBeanObject> getter) {
      return metricsGetter(null, getter);
    }

    /**
     * @param name of getter. This must be unique. The getters having same name will be in same
     *     group and only the latest one gets work.
     * @param getter getter
     * @return this register
     */
    Register metricsGetter(String name, Function<MBeanClient, HasBeanObject> getter);

    Unregister build();
  }

  interface Unregister {
    /** remove the getter */
    void removeGetter();
  }

  interface Node {
    String host();

    int port();
  }

  private static class NodeImpl implements AutoCloseable, Node {
    final MBeanClient client;
    final Map<String, Deque<Function<MBeanClient, HasBeanObject>>> allGetters =
        new ConcurrentHashMap<>();
    final Queue<HasBeanObject> objects = new ConcurrentLinkedQueue<>();
    final int numberOfObjects;
    final AtomicBoolean updating = new AtomicBoolean(false);

    NodeImpl(MBeanClient client, int numberOfObjects) {
      this.client = client;
      this.numberOfObjects = numberOfObjects;
    }

    void updateObjects() {
      // don't process one node by two threads
      if (updating.compareAndSet(false, true)) {
        try {
          allGetters.forEach(
              (name, getters) -> {
                var getter = getters.getLast();
                if (getter != null) {
                  if (objects.size() >= numberOfObjects) objects.poll();
                  objects.offer(getter.apply(client));
                }
              });
        } finally {
          updating.set(false);
        }
      }
    }

    @Override
    public void close() {
      Utils.close(client);
    }

    @Override
    public String host() {
      return client.host();
    }

    @Override
    public int port() {
      return client.port();
    }
  }
}
