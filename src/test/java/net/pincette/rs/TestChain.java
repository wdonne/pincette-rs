package net.pincette.rs;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.TestUtil.values;
import static net.pincette.rs.Util.join;
import static net.pincette.util.Collections.list;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;
import net.pincette.util.State;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestChain {
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
    final List<Integer> data = list(1, 2, 3, 4);
    final List<Integer> committed = new ArrayList<>();

    join(
        with(of(data))
            .commit(
                l -> {
                  committed.addAll(l);
                  return completedFuture(true);
                })
            .buffer(2)
            .get());

    assertEquals(data, committed);
  }

  @Test
  @DisplayName("chain commit2")
  void commit2() {
    final List<Integer> data = list(1, 2, 3, 4);
    final List<Integer> committed = new ArrayList<>();

    join(
        with(of(data))
            .commit(
                l -> {
                  committed.addAll(l);
                  return completedFuture(true);
                })
            .buffer(8)
            .get());

    assertEquals(list(), committed);
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
  @DisplayName("chain flatMap")
  void flatMap() {
    final int max = 500000;
    final List<Integer> list = values(0, max);

    runTest(
        list,
        () ->
            with(of(list))
                .flatMap(i -> with(of(list(i))).mapAsync(v -> supplyAsync(() -> v)).get())
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
  @DisplayName("chain mapAsync 1")
  void mapAsync1() {
    runTest(
        list(2, 3, 4, 5),
        () -> with(of(list(1, 2, 3, 4))).mapAsync(v -> supplyAsync(() -> v + 1)).get());
  }

  @Test
  @DisplayName("chain mapAsync 2")
  void mapAsync2() {
    final List<Integer> values = values(0, 100);

    runTest(values, () -> with(of(values)).mapAsync(v -> supplyAsync(() -> v)).buffer(1000).get());
  }

  @Test
  @DisplayName("chain mapAsync 3")
  void mapAsync3() {
    runTest(
        list(1, 3, 6, 10),
        () ->
            with(of(list(1, 2, 3, 4)))
                .mapAsync((v, p) -> supplyAsync(() -> v + (p != null ? (int) p : 0)))
                .get());
  }

  @Test
  @DisplayName("chain mapAsync 4")
  void mapAsync4() {
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
  @DisplayName("chain mapAsync 5")
  void mapAsync5() {
    final List<Integer> values = values(0, 100);

    runTest(
        values,
        () -> with(of(values)).mapAsyncSequential(v -> supplyAsync(() -> v)).buffer(1000).get());
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
