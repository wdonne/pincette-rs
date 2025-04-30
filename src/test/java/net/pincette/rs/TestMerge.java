package net.pincette.rs;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static net.pincette.rs.Async.mapAsync;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.FlattenList.flattenList;
import static net.pincette.rs.Per.per;
import static net.pincette.rs.Probe.probe;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.asListIter;
import static net.pincette.rs.TestUtil.initLogging;
import static net.pincette.rs.TestUtil.values;
import static net.pincette.rs.Util.LOGGER;
import static net.pincette.rs.Util.asList;
import static net.pincette.rs.Util.subscribe;
import static net.pincette.util.Collections.concat;
import static net.pincette.util.Collections.list;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestMerge {
  @BeforeAll
  static void beforeAll() {
    initLogging();
  }

  private static <T> void runTest(
      final List<?> target, final Supplier<Publisher<T>> publisher, final int times) {
    for (int i = 0; i < times; ++i) {
      LOGGER.finest("runTest asList");
      assertEquals(target, sort(asList(publisher.get(), target.size())));
      LOGGER.finest("runTest asListIter");
      assertEquals(target, sort(asListIter(publisher.get(), target.size())));
    }
  }

  private static <T> List<T> sort(final List<T> list) {
    return list.stream().sorted().toList();
  }

  private static void test(final BinaryOperator<Publisher<Integer>> merge) {
    final List<Integer> first = list(1, 2, 3, 4);
    final List<Integer> second = list(5, 6, 7, 8);

    runTest(concat(first, second), () -> merge.apply(of(first), of(second)), 1000);
  }

  @Test
  @DisplayName("merge1")
  void merge1() {
    test(Merge::of);
  }

  @Test
  @DisplayName("merge2")
  void merge2() {
    test((p1, p2) -> Merge.of(list(p1, p2)));
  }

  @Test
  @DisplayName("merge3")
  void merge3() {
    final Supplier<Processor<Integer, Integer>> after = () -> box(per(100), flattenList());
    final List<Integer> values = values(0, 200000);

    runTest(
        sort(concat(values, values)),
        () -> subscribe(Merge.of(of(values), of(values)), after.get()),
        1);
  }

  @Test
  @DisplayName("merge4")
  void merge4() {
    final Supplier<Processor<Integer, Integer>> after = () -> box(per(100), flattenList());
    final List<Integer> values = values(0, 200000);
    final Supplier<Publisher<Integer>> processor =
        () -> subscribe(of(values), mapAsync(v -> supplyAsync(() -> v)));

    runTest(
        sort(concat(values, values)),
        () -> subscribe(Merge.of(processor.get(), processor.get()), after.get()),
        1);
  }

  @Test
  @DisplayName("merge5")
  void merge5() {
    final Stall<Integer> stall = new Stall<>();
    final List<Integer> values = values(0, 10);

    runTest(
        values,
        () ->
            subscribe(
                Merge.of(of(values), stall),
                probe(
                    n -> {},
                    v -> {
                      if (v == values.size() - 1) {
                        stall.complete();
                      }
                    })),
        1);
  }
}
