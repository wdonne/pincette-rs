package net.pincette.rs;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.TestUtil.values;
import static net.pincette.util.Collections.list;
import static net.pincette.util.StreamUtil.per;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestConcat {
  @Test
  @DisplayName("concat1")
  void concat1() {
    runTest(
        list(0, 1, 2, 3, 4, 5),
        () -> Concat.of(Source.of(list(0, 1)), Source.of(list(2, 3)), Source.of(list(4, 5))));
  }

  @Test
  @DisplayName("concat2")
  void concat2() {
    runTest(list(), () -> Concat.of(list()));
  }

  @Test
  @DisplayName("concat3")
  void concat3() {
    runTest(
        list(0, 1, 2, 3, 4, 5),
        () ->
            with(Concat.of(Source.of(list(0, 1)), Source.of(list(2, 3)), Source.of(list(4, 5))))
                .buffer(20)
                .get());
  }

  @Test
  @DisplayName("concat4")
  void concat4() {
    final List<Integer> values = values(0, 20000);

    runTest(
        values,
        () ->
            Concat.of(
                per(
                        per(values.stream(), 100)
                            .map(Source::of)
                            .map(s -> with(s).mapAsync(i -> supplyAsync(() -> i)).get()),
                        10)
                    .map(Concat::of)
                    .toList()),
        1);
  }
}
