package net.pincette.rs;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static net.pincette.rs.Buffer.buffer;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.initLogging;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.TestUtil.values;
import static net.pincette.rs.Util.devNull;
import static net.pincette.rs.Util.duplicateFilter;
import static net.pincette.rs.Util.tap;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Util.tryToDo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestFanout {
  private static <T> Subscriber<T> subscriber(
      final Consumer<T> onNext, final Runnable onComplete, final int buffer) {
    final Processor<T, T> processor = buffer(buffer);

    with(processor)
        .mapAsync(v -> supplyAsync(() -> v))
        .map(v -> v)
        .get()
        .subscribe(new LambdaSubscriber<>(onNext::accept, onComplete::run));

    return processor;
  }

  private static void test(
      final BiFunction<List<Integer>, CountDownLatch, Subscriber<Integer>> subscriber,
      final BinaryOperator<Subscriber<Integer>> fanout) {
    final List<Integer> first = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(2);
    final List<Integer> values = values(0, 10000);
    final List<Integer> second = new ArrayList<>();

    with(Merge.of(
            with(of(values)).flatMap(Source::of).mapAsync(v -> supplyAsync(() -> v)).get(),
            with(of(values)).flatMap(Source::of).mapAsync(v -> supplyAsync(() -> v)).get()))
        .buffer(2)
        .map(duplicateFilter(v -> v, ofSeconds(1)))
        .get()
        .subscribe(fanout.apply(subscriber.apply(first, latch), subscriber.apply(second, latch)));
    tryToDo(latch::await);
    assertEquals(values, first);
    assertEquals(values, second);
  }

  @BeforeAll
  static void beforeAll() {
    initLogging();
  }

  @Test
  @DisplayName("fanout1")
  void fanout1() {
    test((results, latch) -> new LambdaSubscriber<>(results::add, latch::countDown), Fanout::of);
  }

  @Test
  @DisplayName("fanout2")
  void fanout2() {
    test((results, latch) -> subscriber(results::add, latch::countDown, 1), Fanout::of);
  }

  @Test
  @DisplayName("fanout3")
  void fanout3() {
    test(
        (results, latch) -> subscriber(results::add, latch::countDown, 1),
        (s1, s2) -> Fanout.of(list(s1, s2), v -> v));
  }

  @Test
  @DisplayName("fanout4")
  void fanout4() {
    test((results, latch) -> subscriber(results::add, latch::countDown, 3), Fanout::of);
  }

  @Test
  @DisplayName("fanout5")
  void fanout5() {
    test(
        (results, latch) -> subscriber(results::add, latch::countDown, 4),
        (s1, s2) -> Fanout.of(list(s1, s2), v -> v));
  }

  @Test
  @DisplayName("fanout6")
  void fanout6() {
    runTest(
        values(0, 6), () -> with(of(values(0, 10))).map(tap(devNull())).cancel(v -> v == 5).get());
  }
}
