package net.pincette.rs;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.util.Collections.list;
import static net.pincette.util.ScheduledCompletionStage.supplyAsyncAfter;
import static net.pincette.util.Util.tryToDo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;

class TestChain {
  private static <T> Subscriber<T> subscriber(final Consumer<T> onNext, final Runnable onComplete) {
    final Processor<T, T> processor = new PassThrough<>();

    with(processor)
        .mapAsync(v -> supplyAsync(() -> v))
        .map(v -> v)
        .get()
        .subscribe(new LambdaSubscriber<>(onNext::accept, onComplete::run));

    return processor;
  }

  @Test
  @DisplayName("chain after")
  void after() {
    runTest(list(1, -1), () -> with(of(list(1))).after(-1).get());
  }

  @Test
  @DisplayName("chain before")
  void before() {
    runTest(list(-1, 1), () -> with(of(list(1))).before(-1).get());
  }

  @Test
  @DisplayName("chain buffer")
  void buffer() {
    final List<Integer> values = list(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    runTest(values, () -> with(of(values)).buffer(1).get());
    runTest(values, () -> with(of(values)).buffer(2).get());
    runTest(values, () -> with(of(values)).buffer(3).get());
    runTest(values, () -> with(of(values)).buffer(8).get());
    runTest(values, () -> with(of(values)).buffer(10).get());
    runTest(values, () -> with(of(values)).buffer(20).get());
  }

  @Test
  @DisplayName("chain combinations")
  void combinations() {
    runTest(list(-10, -1, 1, -1, 2), () -> with(of(list(1, 2))).before(-10).separate(-1).get());
    runTest(list(1, -1, 2, -1, -10), () -> with(of(list(1, 2))).after(-10).separate(-1).get());
    runTest(list(-10, 1, -1, 2), () -> with(of(list(1, 2))).separate(-1).before(-10).get());
    runTest(list(1, -1, 2, -10), () -> with(of(list(1, 2))).separate(-1).after(-10).get());
    runTest(
        list(-1, 2, 100, 3, 100, 4, 1000),
        () -> with(of(list(1, 2, 3))).map(v -> v + 1).separate(100).before(-1).after(1000).get());
    runTest(
        list(-1, 100, 2, 100, 3, 100, 4, 100, 1000),
        () -> with(of(list(1, 2, 3))).map(v -> v + 1).before(-1).after(1000).separate(100).get());
    runTest(
        list(-1, 100, 2, 100, 3, 100, 1000),
        () ->
            with(of(list(1, 2, 3)))
                .map(v -> v + 1)
                .filter(v -> v != 4)
                .before(-1)
                .after(1000)
                .separate(100)
                .get());
  }

  @Test
  @DisplayName("chain fanout1")
  void fanout1() {
    final List<Integer> first = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final List<Integer> values = list(1, 2, 3, 4);
    final List<Integer> second = new ArrayList<>();

    of(values)
        .subscribe(
            Fanout.of(
                new LambdaSubscriber<>(first::add, latch::countDown),
                new LambdaSubscriber<>(second::add, latch::countDown)));
    tryToDo(latch::await);
    assertEquals(values, first);
    assertEquals(values, second);
  }

  @Test
  @DisplayName("chain fanout2")
  void fanout2() {
    final List<Integer> first = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final List<Integer> values = list(1, 2, 3, 4);
    final List<Integer> second = new ArrayList<>();

    of(values)
        .subscribe(
            Fanout.of(
                subscriber(first::add, latch::countDown),
                subscriber(second::add, latch::countDown)));
    tryToDo(latch::await);
    assertEquals(values, first);
    assertEquals(values, second);
  }

  @Test
  @DisplayName("chain filter")
  void filter() {
    runTest(list(2, 4), () -> with(of(list(1, 2, 3, 4))).filter(v -> v % 2 == 0).get());
  }

  @Test
  @DisplayName("chain first")
  void first() {
    runTest(list(1), () -> with(of(list(1, 2, 3, 4))).first().get());
    runTest(list(), () -> with(of(list())).first().get());
  }

  @Test
  @DisplayName("chain last")
  void last() {
    runTest(list(4), () -> with(of(list(1, 2, 3, 4))).last().get());
    runTest(list(), () -> with(of(list())).last().get());
  }

  @Test
  @DisplayName("chain mapAsync")
  void mapAsync() {
    runTest(
        list(2, 3, 4, 5),
        () ->
            with(of(list(1, 2, 3, 4)))
                .mapAsync(v -> supplyAsyncAfter(() -> v + 1, ofMillis(10)))
                .get());
  }

  @Test
  @DisplayName("chain notFilter")
  void notFilter() {
    runTest(list(1, 3), () -> with(of(list(1, 2, 3, 4))).notFilter(v -> v % 2 == 0).get());
  }

  @Test
  @DisplayName("chain per")
  void per() {
    final List<Integer> values = list(0, 1, 2, 3, 4);

    runTest(list(list(0), list(1), list(2), list(3), list(4)), () -> with(of(values)).per(1).get());
    runTest(list(list(0, 1), list(2, 3), list(4)), () -> with(of(values)).per(2).get());
    runTest(list(list(0, 1, 2), list(3, 4)), () -> with(of(values)).per(3).get());
    runTest(list(values), () -> with(of(values)).per(10).get());
  }

  @Test
  @DisplayName("chain separator")
  void separator() {
    runTest(list(1, -1, 2, -1, 3), () -> with(of(list(1, 2, 3))).separate(-1).get());
  }

  @Test
  @DisplayName("chain until")
  void until() {
    runTest(list(1), () -> with(of(list(1, 2, 3, 4))).until(v -> v == 1).get());
    runTest(list(1, 2, 3), () -> with(of(list(1, 2, 3, 4))).until(v -> v == 3).get());
  }
}
