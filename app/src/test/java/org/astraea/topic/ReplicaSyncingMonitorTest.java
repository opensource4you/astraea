package org.astraea.topic;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.TopicPartition;
import org.astraea.utils.DataUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ReplicaSyncingMonitorTest {

  private ByteArrayOutputStream mockOutput;
  private final PrintStream stdout = System.out;
  private final List<Runnable> tearDownTasks = new ArrayList<>();

  @BeforeEach
  void setUp() {
    mockOutput = new ByteArrayOutputStream();
    System.setOut(new PrintStream(mockOutput));
  }

  @AfterEach
  void tearDown() {
    tearDownTasks.forEach(Runnable::run);
    tearDownTasks.clear();

    System.setOut(stdout);
  }

  // helper functions
  private static final BiFunction<String, Integer, TopicPartition> topicPartition =
      TopicPartition::new;
  private static final BiFunction<Integer, long[], List<Replica>> replica =
      (count, size) ->
          IntStream.range(0, count)
              .mapToObj(
                  i ->
                      new Replica(
                          i, 0, size[i], i == 0, size[i] == size[0], false, false, "/tmp/log"))
              .collect(Collectors.toUnmodifiableList());

  @Test
  void execute() throws InterruptedException {
    // arrange
    int interval = 10;
    TopicAdmin mockTopicAdmin = mock(TopicAdmin.class);
    when(mockTopicAdmin.topicNames()).thenReturn(Set.of("topic-1", "topic-2", "topic-3"));
    when(mockTopicAdmin.replicas(anySet()))
        .thenReturn(
            Map.of(
                /* progress 0% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 0, 0}),
                topicPartition.apply("topic-2", 0), replica.apply(3, new long[] {100, 0, 0}),
                topicPartition.apply("topic-3", 0), replica.apply(3, new long[] {100, 0, 0})))
        .thenReturn(
            Map.of(
                /* progress 50% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 50, 50}),
                topicPartition.apply("topic-2", 0), replica.apply(3, new long[] {100, 50, 50}),
                topicPartition.apply("topic-3", 0), replica.apply(3, new long[] {100, 50, 50})))
        .thenReturn(
            Map.of(
                /* progress 100% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 100, 100}),
                topicPartition.apply("topic-2", 0), replica.apply(3, new long[] {100, 100, 100}),
                topicPartition.apply("topic-3", 0), replica.apply(3, new long[] {100, 100, 100})));

    Thread executionThread =
        new Thread(
            () -> {
              ReplicaSyncingMonitor.execute(
                  mockTopicAdmin,
                  org.astraea.argument.Argument.parse(
                      new ReplicaSyncingMonitor.Argument(),
                      new String[] {
                        "--bootstrap.servers", "whatever:9092", "--interval", interval + "ms"
                      }));
            });
    tearDownTasks.add(
        () -> {
          if (executionThread.isAlive()) {
            when(mockTopicAdmin.replicas(anySet())).thenThrow(RuntimeException.class);
            executionThread.interrupt();
          }
        });

    // act
    executionThread.start();
    TimeUnit.MILLISECONDS.timedJoin(executionThread, 4 * (interval) + 1000);

    // assert execution will exit
    assertSame(Thread.State.TERMINATED, executionThread.getState());

    // assert important info has been printed
    assertTrue(mockOutput.toString().contains("topic-1"));
    assertTrue(mockOutput.toString().contains("topic-2"));
    assertTrue(mockOutput.toString().contains("topic-3"));
    assertTrue(mockOutput.toString().contains("Every replica is synced"));
  }

  @Test
  void executeWithKeepTrack() throws InterruptedException {
    // arrange
    int interval = 10;
    TopicAdmin mockTopicAdmin = mock(TopicAdmin.class);
    when(mockTopicAdmin.topicNames()).thenReturn(Set.of("topic-1"));
    when(mockTopicAdmin.replicas(anySet()))
        .thenReturn(
            Map.of(
                /* progress 0% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 0, 0})))
        .thenReturn(
            Map.of(
                /* progress 50% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 50, 50})))
        .thenReturn(
            Map.of(
                /* progress 100% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 100, 100})))
        .thenReturn(
            Map.of(
                /* progress 100%, --track should not exit when progress reach 100% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 100, 100})))
        .thenReturn(
            Map.of(
                /* progress 0%, another alert happened */
                topicPartition.apply("topic-1", 0),
                replica.apply(4, new long[] {100, 100, 100, 0})))
        .thenReturn(
            Map.of(
                /* progress 50% */
                topicPartition.apply("topic-1", 0),
                replica.apply(4, new long[] {100, 100, 100, 50})))
        .thenReturn(
            Map.of(
                /* progress 100% */
                topicPartition.apply("topic-1", 0),
                replica.apply(4, new long[] {100, 100, 100, 100})));

    Thread executionThread =
        new Thread(
            () -> {
              try {
                ReplicaSyncingMonitor.execute(
                    mockTopicAdmin,
                    org.astraea.argument.Argument.parse(
                        new ReplicaSyncingMonitor.Argument(),
                        new String[] {
                          "--bootstrap.servers",
                          "whatever:9092",
                          "--track",
                          "--interval",
                          interval + "ms"
                        }));
              } catch (Exception e) {
                // swallow interrupted error
              }
            });
    tearDownTasks.add(
        () -> {
          if (executionThread.isAlive()) {
            when(mockTopicAdmin.replicas(anySet())).thenThrow(RuntimeException.class);
            executionThread.interrupt();
          }
        });

    // act
    executionThread.start();
    TimeUnit.MILLISECONDS.timedJoin(executionThread, 7 * (interval) + 1000);

    // assert execution will not exit even if all replicas are synced
    assertNotEquals(Thread.State.TERMINATED, executionThread.getState());

    // assert TopicAdmin#replicas call multiple times
    verify(mockTopicAdmin, atLeast(4)).replicas(anySet());

    // assert important info has been printed
    assertTrue(mockOutput.toString().contains("topic-1"));
    assertTrue(mockOutput.toString().contains("Every replica is synced"));
  }

  @Test
  void executeWithTopic() {
    // arrange
    int interval = 10;
    TopicAdmin mockTopicAdmin = mock(TopicAdmin.class);
    when(mockTopicAdmin.replicas(Set.of("target-topic")))
        .thenReturn(
            Map.of(
                topicPartition.apply("target-topic", 0),
                replica.apply(3, new long[] {100, 100, 100})));

    Runnable execution =
        () -> {
          try {
            ReplicaSyncingMonitor.execute(
                mockTopicAdmin,
                org.astraea.argument.Argument.parse(
                    new ReplicaSyncingMonitor.Argument(),
                    new String[] {
                      "--bootstrap.servers",
                      "whatever:9092",
                      "--topics",
                      "target-topic",
                      "--interval",
                      interval + "ms"
                    }));
          } catch (IllegalStateException e) {
            // immediate fail due to bad behavior of --topic flag
            fail();
          }
        };

    // act
    execution.run();

    // assert TopicAdmin#replicas call at least 1 times with Set.of("target-topic")
    verify(mockTopicAdmin, atLeast(1)).replicas(Set.of("target-topic"));
  }

  @Test
  void findNonSyncedTopicPartition() {
    // arrange
    final TopicAdmin mockTopicAdmin = mock(TopicAdmin.class);
    final Set<String> topics = Set.of("topic1", "topic2");
    final List<Replica> replicaList1 =
        List.of(
            new Replica(0, 0, 0, true, true, false, false, "/tmp/broker0/logA"),
            new Replica(1, 0, 0, true, true, false, false, "/tmp/broker1/logA"));
    final List<Replica> replicaList2 =
        List.of(
            new Replica(0, 0, 100, true, false, false, false, "/tmp/broker0/logB"),
            new Replica(1, 0, 100, true, true, false, false, "/tmp/broker1/logB"));
    when(mockTopicAdmin.replicas(any()))
        .thenReturn(
            Map.of(
                new TopicPartition("topic1", 0), replicaList1,
                new TopicPartition("topic2", 0), replicaList2,
                new TopicPartition("topic2", 1), replicaList2));

    // act
    Set<TopicPartition> nonSyncedTopicPartition =
        ReplicaSyncingMonitor.findNonSyncedTopicPartition(mockTopicAdmin, topics);

    // assert
    assertTrue(nonSyncedTopicPartition.contains(new TopicPartition("topic2", 1)));
    assertTrue(nonSyncedTopicPartition.contains(new TopicPartition("topic2", 0)));
    assertFalse(nonSyncedTopicPartition.contains(new TopicPartition("topic1", 0)));
  }

  @Test
  void ensureArgumentFlagExists() {
    // arrange
    var correct =
        Set.of(
            "--bootstrap.servers localhost:5566",
            "--bootstrap.servers localhost:5566 --track",
            "--bootstrap.servers localhost:5566 --topics my-topic --track",
            "--bootstrap.servers localhost:5566 --interval 1234");
    var incorrect =
        Set.of(
            "--bootstrap.servers localhost:5566 --whatever",
            "--bootstrap.servers localhost:5566 sad",
            "wuewuewuewue",
            "--server");

    // act
    Consumer<String[]> execution =
        (String[] args) ->
            org.astraea.argument.Argument.parse(new ReplicaSyncingMonitor.Argument(), args);

    // assert
    correct.stream()
        .map(args -> args.split(" "))
        .forEach(args -> assertDoesNotThrow(() -> execution.accept(args)));
    incorrect.stream()
        .map(args -> args.split(" "))
        .forEach(args -> assertThrows(Exception.class, () -> execution.accept(args)));
  }

  @ParameterizedTest
  @CsvSource(
      delimiterString = ",",
      value = {
        // leader, previous, current, interval, dataRatePerSec, Progress, Remaining, test-purpose
        "  100   , 0       , 50     , 1000    , 50.0          , 50      , 1        , test",
        "  200   , 0       , 100    , 1000    , 100.0         , 50      , 1        , test",
        "  100   , 25      , 50     , 1000    , 25.0          , 50      , 2        , test",
        "  100   , 0       , 10     , 10000   , 1.0           , 10      , 90       , 10 sec interval",
        "  100   , 50      , 50     , 1000    , 0.0           , 50      , -1       , stalled progress",
      })
  void dataRate(
      long leaderSize,
      long previousSize,
      long currentSize,
      int interval,
      double expectedDataRatePerSec,
      double expectedProgress,
      int expectedRemainingTime) {
    // act
    var progress =
        new ReplicaSyncingMonitor.ProgressInfo(
            DataUnit.Byte.of(leaderSize),
            DataUnit.Byte.of(previousSize),
            DataUnit.Byte.of(currentSize),
            Duration.ofMillis(interval));

    // assert
    assertEquals(expectedDataRatePerSec, progress.dataRate(DataUnit.Byte, ChronoUnit.SECONDS));
    assertEquals(expectedProgress, progress.progress());
    assertEquals(Duration.ofSeconds(expectedRemainingTime), progress.estimateFinishTime());
  }
}
