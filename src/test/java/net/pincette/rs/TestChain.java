package net.pincette.rs;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofNanos;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.FlattenList.flattenList;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.initLogging;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.TestUtil.values;
import static net.pincette.rs.Util.generate;
import static net.pincette.rs.Util.join;
import static net.pincette.util.Collections.list;
import static net.pincette.util.ScheduledCompletionStage.supplyAsyncAfter;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;
import net.pincette.util.State;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestChain {
  @BeforeAll
  public static void beforeAll() {
    initLogging();
  }

  private static void bufferTest(final int size) {
    final List<Integer> values = values(0, 10);

    runTest(values, () -> with(of(values)).buffer(size).get());
  }

  private static void flattenTest1(final Supplier<Processor<List<Integer>, Integer>> processor) {
    final List<Integer> values = values(0, 10);

    runTest(
        values,
        () -> with(of(values)).per(3).map(processor.get()).map(v -> v + 1).map(v -> v - 1).get());

    runTest(
        values,
        () ->
            with(of(values))
                .per(3)
                .map(processor.get())
                .buffer(4)
                .map(v -> v + 1)
                .map(v -> v - 1)
                .get());

    runTest(
        values,
        () ->
            with(of(values))
                .per(3)
                .map(processor.get())
                .buffer(2)
                .map(v -> v + 1)
                .map(v -> v - 1)
                .get());
  }

  private static void flattenTest2(final Supplier<Processor<List<Integer>, Integer>> processor) {
    final List<Integer> values = values(0, 10);

    runTest(
        values,
        () ->
            with(of(values))
                .per(3)
                .map(processor.get())
                .buffer(4)
                .map(v -> v + 1)
                .map(v -> v - 1)
                .get());
  }

  private static void flattenTest3(final Supplier<Processor<List<Integer>, Integer>> processor) {
    final List<Integer> values = values(0, 10);

    runTest(
        values,
        () ->
            with(of(values))
                .per(3)
                .map(processor.get())
                .buffer(2)
                .map(v -> v + 1)
                .map(v -> v - 1)
                .get());
  }

  private static void mapAsync(final int valueSize, final int bufferSize) {
    final List<Integer> values = values(0, valueSize);

    runTest(
        values,
        () -> with(of(values)).mapAsync(v -> supplyAsync(() -> v)).buffer(bufferSize).get());
  }

  private static void mapAsyncSequential(final int valueSize, final int bufferSize) {
    final List<Integer> values = values(0, valueSize);

    runTest(
        values,
        () ->
            with(of(values))
                .mapAsyncSequential(v -> supplyAsync(() -> v))
                .buffer(bufferSize)
                .get());
  }

  @Test
  @DisplayName("chain after")
  void after() {
    runTest(list(1, -1), () -> with(of(list(1))).after(-1).get());
  }

  @Test
  @DisplayName("chain after before empty 1")
  void afterBeforeEmpty1() {
    runTest(list(1, -1), () -> with(of(list())).before(1).after(-1).get());
  }

  @Test
  @DisplayName("chain after before empty 2")
  void afterBeforeEmpty2() {
    runTest(list(1, -1), () -> with(of(list())).after(-1).before(1).get());
  }

  @Test
  @DisplayName("chain after before many 1")
  void afterBeforeMany1() {
    runTest(list(1, 2, 3, -1), () -> with(of(list(2, 3))).beforeIfMany(1).afterIfMany(-1).get());
  }

  @Test
  @DisplayName("chain after before many 2")
  void afterBeforeMany2() {
    runTest(list(2), () -> with(of(list(2))).beforeIfMany(1).afterIfMany(-1).get());
  }

  @Test
  @DisplayName("chain after before many 3")
  void afterBeforeMany3() {
    runTest(list(), () -> with(of(list())).beforeIfMany(1).afterIfMany(-1).get());
  }

  @Test
  @DisplayName("chain before")
  void before() {
    runTest(list(-1, 1), () -> with(of(list(1))).before(-1).get());
  }

  @Test
  @DisplayName("chain buffer1")
  void buffer1() {
    bufferTest(1);
  }

  @Test
  @DisplayName("chain buffer2")
  void buffer2() {
    bufferTest(2);
  }

  @Test
  @DisplayName("chain buffer3")
  void buffer3() {
    bufferTest(3);
  }

  @Test
  @DisplayName("chain buffer4")
  void buffer4() {
    bufferTest(8);
  }

  @Test
  @DisplayName("chain buffer5")
  void buffer5() {
    bufferTest(10);
  }

  @Test
  @DisplayName("chain buffer6")
  void buffer6() {
    bufferTest(20);
  }

  @Test
  @DisplayName("cancel")
  void cancel() {
    runTest(
        list(1, 2),
        () -> {
          final State<Integer> value = new State<>(0);

          return with(generate(() -> value.set(value.get() + 1))).cancel(v -> v == 2).get();
        },
        1000,
        false); // With iter and hence another thread there is no control over when it stops.
  }

  @Test
  @DisplayName("chain combinations1")
  void combinations1() {
    runTest(list(-10, -1, 1, -1, 2), () -> with(of(list(1, 2))).before(-10).separate(-1).get());
  }

  @Test
  @DisplayName("chain combinations2")
  void combinations2() {
    runTest(list(1, -1, 2, -1, -10), () -> with(of(list(1, 2))).after(-10).separate(-1).get());
  }

  @Test
  @DisplayName("chain combinations3")
  void combinations3() {
    runTest(list(-10, 1, -1, 2), () -> with(of(list(1, 2))).separate(-1).before(-10).get());
  }

  @Test
  @DisplayName("chain combinations4")
  void combinations4() {
    runTest(list(1, -1, 2, -10), () -> with(of(list(1, 2))).separate(-1).after(-10).get());
  }

  @Test
  @DisplayName("chain combinations5")
  void combinations5() {
    runTest(
        list(-1, 2, 100, 3, 100, 4, 1000),
        () -> with(of(list(1, 2, 3))).map(v -> v + 1).separate(100).before(-1).after(1000).get());
  }

  @Test
  @DisplayName("chain combinations6")
  void combinations6() {
    runTest(
        list(-1, 100, 2, 100, 3, 100, 4, 100, 1000),
        () -> with(of(list(1, 2, 3))).map(v -> v + 1).before(-1).after(1000).separate(100).get());
  }

  @Test
  @DisplayName("chain combinations7")
  void combinations7() {
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
  @DisplayName("chain commit1")
  void commit1() {
    commit(list(1, 2, 3, 4), 2);
  }

  @Test
  @DisplayName("chain commit2")
  void commit2() {
    commit(list(1, 2, 3, 4), 8);
  }

  @Test
  @DisplayName("chain commit3")
  void commit3() {
    commit(list(1, 2, 3, 4), 1);
  }

  @Test
  @DisplayName("chain commit4")
  void commit4() {
    commit(list(1, 2, 3, 4), 4);
  }

  private void commit(final List<Integer> data, final int bufferSize) {
    final List<Integer> committed = new ArrayList<>();

    join(
        with(of(data))
            .commit(
                l -> {
                  committed.addAll(l);
                  return completedFuture(true);
                })
            .buffer(bufferSize, ofNanos(0)) // Keep it going if the buffer is larger than the data.
            .get());

    assertEquals(data, committed);
  }

  @Test
  @DisplayName("chain filter")
  void filter() {
    runTest(list(2, 4), () -> with(of(list(1, 2, 3, 4))).filter(v -> v % 2 == 0).get());
  }

  @Test
  @DisplayName("chain first1")
  void first1() {
    runTest(list(1), () -> with(of(list(1, 2, 3, 4))).first().get());
  }

  @Test
  @DisplayName("chain first2")
  void first2() {
    runTest(list(), () -> with(of(list())).first().get());
  }

  @Test
  @DisplayName("chain flatMap1")
  void flatMap1() {
    final List<Integer> list = values(0, 500000);

    runTest(
        list,
        () ->
            with(of(list))
                .flatMap(i -> with(of(list(i))).mapAsync(v -> supplyAsync(() -> v)).get())
                .get(),
        1);
  }

  @Test
  @DisplayName("chain flatMap2")
  void flatMap2() {
    final List<List<Integer>> lists = list(values(0, 3), list(), values(3, 6));

    runTest(
        values(0, 6),
        () ->
            with(of(values(0, 3)))
                .flatMap(i -> with(of(lists.get(i))).mapAsync(v -> supplyAsync(() -> v)).get())
                .get(),
        1);
  }

  @Test
  @DisplayName("chain flatten1")
  void flatten1() {
    flattenTest1(FlattenList::flattenList);
  }

  @Test
  @DisplayName("chain flatten2")
  void flatten2() {
    flattenTest2(FlattenList::flattenList);
  }

  @Test
  @DisplayName("chain flatten3")
  void flatten3() {
    flattenTest3(FlattenList::flattenList);
  }

  @Test
  @DisplayName("chain flatten4")
  void flatten4() {
    flattenTest1(() -> box(map(Source::of), Flatten.flatten()));
  }

  @Test
  @DisplayName("chain flatten5")
  void flatten5() {
    flattenTest2(() -> box(map(Source::of), Flatten.flatten()));
  }

  @Test
  @DisplayName("chain flatten6")
  void flatten6() {
    flattenTest3(() -> box(map(Source::of), Flatten.flatten()));
  }

  @Test
  @DisplayName("chain headTail")
  void headTail() {
    final State<Integer> head = new State<>();

    runTest(list(2, 3, 4), () -> with(of(list(1, 2, 3, 4))).headTail(head::set, v -> v).get());
  }

  @Test
  @DisplayName("chain last1")
  void last1() {
    runTest(list(4), () -> with(of(list(1, 2, 3, 4))).last().get());
  }

  @Test
  @DisplayName("chain last2")
  void last2() {
    runTest(list(), () -> with(of(list())).last().get());
  }

  @Test
  @DisplayName("chain map")
  void mapInt() {
    final List<Integer> values = values(0, 1000);

    runTest(values, () -> with(of(values)).map(v -> v).buffer(1000).get());
  }

  @Test
  @DisplayName("chain mapAsync 1")
  void mapAsync1() {
    runTest(
        list(2, 3, 4, 5),
        () -> with(of(list(1, 2, 3, 4))).mapAsync(v -> supplyAsync(() -> v + 1)).get());
  }

  @Test
  @DisplayName("chain mapAsync 2")
  void mapAsync2() {
    mapAsync(100, 1000);
  }

  @Test
  @DisplayName("chain mapAsync 3")
  void mapAsync3() {
    mapAsync(1000, 1);
  }

  @Test
  @DisplayName("chain mapAsync 4")
  void mapAsync4() {
    mapAsync(1000, 10);
  }

  @Test
  @DisplayName("chain mapAsync 5")
  void mapAsync5() {
    runTest(
        list(1, 3, 6, 10),
        () ->
            with(of(list(1, 2, 3, 4)))
                .mapAsync((v, p) -> supplyAsync(() -> v + (p != null ? (int) p : 0)))
                .get());
  }

  @Test
  @DisplayName("chain mapAsync 6")
  void mapAsync6() {
    final List<Integer> values = values(0, 100);

    runTest(
        values,
        () ->
            with(of(values))
                .mapAsync((Integer v, Integer p) -> supplyAsync(() -> v))
                .buffer(1000)
                .get());
  }

  @Test
  @DisplayName("chain mapAsyncSequential 1")
  void mapAsyncSequential1() {
    mapAsyncSequential(100, 1000);
  }

  @Test
  @DisplayName("chain mapAsyncSequential 2")
  void mapAsyncSequential2() {
    mapAsyncSequential(100, 1);
  }

  @Test
  @DisplayName("chain notFilter")
  void notFilter() {
    runTest(list(1, 3), () -> with(of(list(1, 2, 3, 4))).notFilter(v -> v % 2 == 0).get());
  }

  @Test
  @DisplayName("chain per1")
  void per1() {
    runTest(
        list(list(0), list(1), list(2), list(3), list(4)),
        () -> with(of(values(0, 5))).per(1).get());
  }

  @Test
  @DisplayName("chain per2")
  void per2() {
    runTest(list(list(0, 1), list(2, 3), list(4)), () -> with(of(values(0, 5))).per(2).get());
  }

  @Test
  @DisplayName("chain per3")
  void per3() {
    runTest(list(list(0, 1, 2), list(3, 4)), () -> with(of(values(0, 5))).per(3).get());
  }

  @Test
  @DisplayName("chain per4")
  void per4() {
    runTest(list(list(0, 1, 2), list(3, 4)), () -> with(of(values(0, 5))).buffer(10).per(3).get());
  }

  @Test
  @DisplayName("chain per5")
  void per5() {
    runTest(list(values(0, 5)), () -> with(of(values(0, 5))).per(10).get());
  }

  @Test
  @DisplayName("chain per6")
  void per6() {
    final Random random = new Random();
    final List<Integer> values = values(0, 10000);

    runTest(
        values,
        () ->
            with(of(values))
                .mapAsync(
                    v ->
                        Optional.of(random.nextInt(100))
                            .filter(i -> i == 33)
                            .map(i -> supplyAsyncAfter(() -> v, ofMillis(100)))
                            .orElseGet(() -> completedFuture(v)))
                .per(10, ofMillis(50))
                .map(flattenList())
                .get(),
        1);
  }

  @Test
  @DisplayName("chain separator")
  void separator() {
    runTest(list(1, -1, 2, -1, 3), () -> with(of(list(1, 2, 3))).separate(-1).get());
  }

  @Test
  @DisplayName("chain split")
  void split() {
    final List<Integer> values = values(0, 5);

    runTest(values, () -> with(of(values)).map(v -> v + 1).split().map(v -> v - 1).get());
  }

  @Test
  @DisplayName("chain until1")
  void until1() {
    runTest(list(1), () -> with(of(list(1, 2, 3, 4))).until(v -> v == 1).get());
  }

  @Test
  @DisplayName("chain until2")
  void until2() {
    runTest(list(1, 2, 3), () -> with(of(list(1, 2, 3, 4))).until(v -> v == 3).get());
  }
}
