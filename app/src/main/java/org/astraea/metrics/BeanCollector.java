package org.astraea.metrics;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.management.remote.JMXServiceURL;
import org.astraea.Utils;
import org.astraea.concurrent.ThreadPool;
import org.astraea.metrics.jmx.MBeanClient;

public class BeanCollector implements AutoCloseable {
  private static final int MAX_OBJECTS = 30;
  private final Map<Integer, Set<NodeImpl>> allNodes = new ConcurrentHashMap<>();
  private final ThreadPool pool;

  public BeanCollector() {
    this(Duration.ofSeconds(1), 2);
  }

  public BeanCollector(Duration interval, int numberOfThreads) {
    this.pool =
        ThreadPool.builder()
            .executors(
                IntStream.range(0, numberOfThreads)
                    .mapToObj(
                        i ->
                            new ThreadPool.Executor() {
                              @Override
                              public State execute() throws InterruptedException {
                                nodes(i).forEach(NodeImpl::updateObjects);
                                TimeUnit.MILLISECONDS.sleep(interval.toMillis());
                                return ThreadPool.Executor.State.RUNNING;
                              }

                              @Override
                              public void close() {
                                var nodes = allNodes.remove(i);
                                if (nodes != null) nodes.forEach(NodeImpl::close);
                              }
                            })
                    .collect(Collectors.toList()))
            .build();
  }

  private Set<NodeImpl> nodes(int index) {
    return allNodes.computeIfAbsent(
        index,
        ignored ->
            new ConcurrentSkipListSet<>(
                Comparator.comparing(NodeImpl::host).thenComparing(NodeImpl::port)));
  }

  /** @return the monitored host/port */
  public List<Node> nodes() {
    return allNodes.values().stream()
        .flatMap(ns -> ns.stream().map(n -> (Node) n))
        .collect(Collectors.toList());
  }

  /**
   * @param host target host
   * @param port target port
   * @return the objects from target host/port
   */
  public List<HasBeanObject> objects(String host, int port) {
    return allNodes.values().stream()
        .flatMap(
            ns ->
                ns.stream()
                    .filter(n -> n.host().equals(host) && n.port() == port)
                    .flatMap(n -> n.objects.stream()))
        .collect(Collectors.toList());
  }

  public Map<Node, List<HasBeanObject>> objects() {
    return nodes().stream()
        .collect(Collectors.toMap(Function.identity(), n -> objects(n.host(), n.port())));
  }

  /** @return the number of all objects */
  public int size() {
    return allNodes.values().stream()
        .mapToInt(ns -> ns.stream().mapToInt(n -> n.objects.size()).sum())
        .sum();
  }

  @Override
  public void close() throws Exception {
    pool.close();
    pool.waitAll();
  }

  public void addClient(JMXServiceURL url, Function<MBeanClient, HasBeanObject> getter) {
    if (pool.isClosed()) throw new RuntimeException("this is closed!!!");
    var existentNode =
        allNodes.values().stream()
            .flatMap(
                ns ->
                    ns.stream()
                        .filter(n -> n.host().equals(url.getHost()) && n.port() == url.getPort()))
            .findFirst();
    // reuse the existent client to get metrics
    if (existentNode.isPresent()) existentNode.get().getters.add(getter);
    else
      nodes((int) (Math.random() * pool.size()))
          .add(new NodeImpl(new MBeanClient(url), MAX_OBJECTS, getter));
  }

  interface Node {
    String host();

    int port();
  }

  private static class NodeImpl implements AutoCloseable, Node {
    final MBeanClient client;
    final List<Function<MBeanClient, HasBeanObject>> getters = new CopyOnWriteArrayList<>();
    final Queue<HasBeanObject> objects;
    final int numberOfObjects;

    NodeImpl(MBeanClient client, int numberOfObjects, Function<MBeanClient, HasBeanObject> getter) {
      this.client = client;
      this.getters.add(getter);
      this.objects = new LinkedBlockingQueue<>(numberOfObjects);
      this.numberOfObjects = numberOfObjects;
    }

    void updateObjects() {
      getters.forEach(
          getter -> {
            if (objects.size() >= numberOfObjects) objects.poll();
            objects.offer(getter.apply(client));
          });
    }

    @Override
    public void close() {
      Utils.close(client);
    }

    @Override
    public String host() {
      return client.getAddress().getHost();
    }

    @Override
    public int port() {
      return client.getAddress().getPort();
    }
  }
}
