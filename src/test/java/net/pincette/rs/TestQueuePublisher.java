package net.pincette.rs;

import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.TestUtil.values;
import static net.pincette.util.StreamUtil.supplyAsyncStream;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestQueuePublisher {
  private static final List<Integer> VALUES = values(0, 10000);

  @Test
  @DisplayName("queue publisher 1")
  void queuePublisher1() {
    runTest(
        VALUES,
        () -> {
          final QueuePublisher<Integer> publisher = new QueuePublisher<>();

          VALUES.forEach(v -> publisher.getQueue().add(v));
          publisher.close();

          return publisher;
        },
        100);
  }

  @Test
  @DisplayName("queue publisher 2")
  void queuePublisher2() {
    runTest(
        VALUES,
        () -> {
          final QueuePublisher<Integer> publisher = new QueuePublisher<>();

          publisher.getQueue().addAll(VALUES);
          publisher.close();

          return publisher;
        },
        100);
  }

  @Test
  @DisplayName("queue publisher 3")
  void queuePublisher3() {
    runTest(
        VALUES,
        () -> {
          final QueuePublisher<Integer> publisher = new QueuePublisher<>();

          new Thread(
                  () -> {
                    VALUES.forEach(v -> publisher.getQueue().add(v));
                    publisher.close();
                  })
              .start();

          return publisher;
        },
        100);
  }

  @Test
  @DisplayName("queue publisher 4")
  void queuePublisher4() {
    runTest(
        VALUES,
        () -> {
          final QueuePublisher<Integer> publisher = new QueuePublisher<>();

          supplyAsyncStream(
                  VALUES.stream()
                      .map(
                          v ->
                              () -> {
                                publisher.getQueue().add(v);

                                return v;
                              }))
              .thenAccept(s -> publisher.close());

          return publisher;
        },
        100);
  }

  @Test
  @DisplayName("queue publisher 5")
  void queuePublisher5() {
    runTest(
        VALUES,
        () -> {
          final Queue<Integer> queue = new ArrayDeque<>(VALUES);

          return new QueuePublisher<>(
              (p, r) -> {
                for (int i = 0; i < r && !queue.isEmpty(); ++i) {
                  p.getQueue().add(queue.remove());
                }

                if (queue.isEmpty()) {
                  p.close();
                }
              });
        },
        1000);
  }
}
