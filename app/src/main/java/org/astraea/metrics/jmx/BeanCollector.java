package org.astraea.metrics.jmx;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.Utils;
import org.astraea.concurrent.ThreadPool;
import org.astraea.metrics.HasBeanObject;

public class BeanCollector implements AutoCloseable {
  private static final int MAX_OBJECTS = 30;
  private final Map<Integer, Set<NodeMetrics>> allNodes = new ConcurrentHashMap<>();
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
                                allNodes(i).forEach(NodeMetrics::updateObjects);
                                TimeUnit.MILLISECONDS.sleep(interval.toMillis());
                                return ThreadPool.Executor.State.RUNNING;
                              }

                              @Override
                              public void close() {
                                var nodes = allNodes.remove(i);
                                if (nodes != null) nodes.forEach(NodeMetrics::close);
                              }
                            })
                    .collect(Collectors.toList()))
            .build();
  }

  private Set<NodeMetrics> allNodes(int index) {
    return allNodes.computeIfAbsent(index, ignored -> Collections.synchronizedSet(new HashSet<>()));
  }

  /** @return the monitored host/port */
  public Set<Map.Entry<String, Integer>> nodesID() {
    return allNodes.values().stream()
        .flatMap(ns -> ns.stream().map(n -> Map.entry(n.host(), n.port())))
        .collect(Collectors.toSet());
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

  public Map<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>> nodesObjects() {
    return nodeMetrics().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().stream()
                        .map(n -> new ArrayList<>(n.objects))
                        .collect(
                            Collectors.toMap(
                                list ->
                                    list.stream()
                                        .map(
                                            mbean ->
                                                new ArrayList<>(
                                                        mbean.beanObject().getProperties().values())
                                                    .get(0))
                                        .collect(Collectors.toList())
                                        .get(0),
                                Function.identity()))));
  }

  public List<NodeMetrics> nodes(String host, int port) {
    return allNodes.values().stream()
        .flatMap(ns -> ns.stream().filter(n -> n.host().equals(host) && n.port() == port))
        .collect(Collectors.toList());
  }

  public Map<Map.Entry<String, Integer>, List<HasBeanObject>> objects() {
    return nodesID().stream()
        .collect(Collectors.toMap(Function.identity(), n -> objects(n.getKey(), n.getValue())));
  }

  public Map<Map.Entry<String, Integer>, List<NodeMetrics>> nodeMetrics() {
    return nodesID().stream()
        .collect(Collectors.toMap(Function.identity(), n -> nodes(n.getKey(), n.getValue())));
  }

  //  public Map<String,List<HasBeanObject>> nodeMetrics (String host, int port) {
  //      var test = objects(host, port).stream().collect(Collectors.toMap(mbean ->
  //              new ArrayList<>(mbean.beanObject().getProperties().values())
  //                      .get(0), list -> list ));
  //  }

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
    allNodes((int) (Math.random() * pool.size())).add(new NodeMetrics(client, MAX_OBJECTS, getter));
  }

  private static class NodeMetrics implements AutoCloseable {
    final MBeanClient client;
    final Function<MBeanClient, HasBeanObject> getter;
    final Queue<HasBeanObject> objects;
    final int numberOfObjects;

    NodeMetrics(
        MBeanClient client, int numberOfObjects, Function<MBeanClient, HasBeanObject> getter) {
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
