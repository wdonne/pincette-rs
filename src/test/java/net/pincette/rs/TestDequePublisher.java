package net.pincette.rs;

import static java.util.stream.Collectors.toList;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.TestUtil.values;
import static net.pincette.util.Collections.reverse;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.StreamUtil.supplyAsyncStream;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestDequePublisher {
  private static final List<Integer> VALUES = values(0, 10000);

  @Test
  @DisplayName("deque publisher 1")
  void dequePublisher1() {
    runTest(
        VALUES,
        () -> {
          final DequePublisher<Integer> publisher = new DequePublisher<>();

          VALUES.forEach(v -> publisher.getDeque().addFirst(v));
          publisher.close();

          return publisher;
        },
        100);
  }

  @Test
  @DisplayName("deque publisher 2")
  void dequePublisher2() {
    runTest(
        VALUES,
        () -> {
          final DequePublisher<Integer> publisher = new DequePublisher<>();

          publisher.getDeque().addAll(stream(reverse(VALUES)).collect(toList()));
          publisher.close();

          return publisher;
        },
        100);
  }

  @Test
  @DisplayName("deque publisher 3")
  void dequePublisher3() {
    runTest(
        VALUES,
        () -> {
          final DequePublisher<Integer> publisher = new DequePublisher<>();

          new Thread(
                  () -> {
                    VALUES.forEach(v -> publisher.getDeque().addFirst(v));
                    publisher.close();
                  })
              .start();

          return publisher;
        },
        100);
  }

  @Test
  @DisplayName("deque publisher 4")
  void dequePublisher4() {
    runTest(
        VALUES,
        () -> {
          final DequePublisher<Integer> publisher = new DequePublisher<>();

          supplyAsyncStream(
                  VALUES.stream()
                      .map(
                          v ->
                              () -> {
                                publisher.getDeque().addFirst(v);

                                return v;
                              }))
              .thenAccept(s -> publisher.close());

          return publisher;
        },
        100);
  }
}
