package net.pincette.rs;

import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.util.Collections.list;

import java.util.List;
import java.util.concurrent.Flow.Processor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestCombine {
  @Test
  @DisplayName("combine")
  void combine() {
    final List<Integer> values = list(0, 1, 2, 3);

    runTest(
        values,
        () -> {
          final Processor<Integer, Integer> first = map(v -> v + 1);
          final Processor<Integer, Integer> second = map(v -> v - 1);

          first.subscribe(second);

          return with(of(values)).map(Combine.combine(first, second)).get();
        });
  }
}
