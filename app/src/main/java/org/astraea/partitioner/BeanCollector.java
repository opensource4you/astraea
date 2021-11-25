package org.astraea.partitioner;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.Utils;
import org.astraea.concurrent.ThreadPool;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.jmx.MBeanClient;

public class BeanCollector implements AutoCloseable {
  private static final int MAX_OBJECTS = 30;
  private final Map<Integer, Set<Node>> allNodes = new ConcurrentHashMap<>();
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
                                nodes(i).forEach(Node::updateObjects);
                                TimeUnit.MILLISECONDS.sleep(interval.toMillis());
                                return ThreadPool.Executor.State.RUNNING;
                              }

                              @Override
                              public void close() {
                                var nodes = allNodes.remove(i);
                                if (nodes != null) nodes.forEach(Node::close);
                              }
                            })
                    .collect(Collectors.toList()))
            .build();
  }

  private Set<Node> nodes(int index) {
    return allNodes.computeIfAbsent(
        index,
        ignored ->
            new ConcurrentSkipListSet<>(
                Comparator.comparing(Node::host).thenComparing(Node::port)));
  }

  /** @return the monitored host/port */
  public List<Map.Entry<String, Integer>> nodes() {
    return allNodes.values().stream()
        .flatMap(ns -> ns.stream().map(n -> Map.entry(n.host(), n.port())))
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

  public Map<Map.Entry<String, Integer>, List<HasBeanObject>> objects() {
    return nodes().stream()
        .collect(Collectors.toMap(Function.identity(), n -> objects(n.getKey(), n.getValue())));
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

  public void addClient(MBeanClient client, Function<MBeanClient, HasBeanObject> getter) {
    if (pool.isClosed()) throw new RuntimeException("this is closed!!!");
    nodes((int) (Math.random() * pool.size())).add(new Node(client, MAX_OBJECTS, getter));
  }

  private static class Node implements AutoCloseable {
    final MBeanClient client;
    final Function<MBeanClient, HasBeanObject> getter;
    final Queue<HasBeanObject> objects;
    final int numberOfObjects;

    Node(MBeanClient client, int numberOfObjects, Function<MBeanClient, HasBeanObject> getter) {
      this.client = client;
      this.getter = getter;
      this.objects = new LinkedBlockingQueue<>(numberOfObjects);
      this.numberOfObjects = numberOfObjects;
    }

    void updateObjects() {
      var newObject = getter.apply(client);
      if (objects.size() >= numberOfObjects) objects.poll();
      objects.offer(newObject);
    }

    @Override
    public void close() {
      Utils.close(client);
    }

    String host() {
      return client.getAddress().getHost();
    }

    int port() {
      return client.getAddress().getPort();
    }
  }
}
