package net.pincette.rs;

import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.util.Collections.list;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestPipe {
  @Test
  @DisplayName("pipe")
  void pipe() {
    final List<Integer> values = list(0, 1, 2, 3);

    runTest(
        values,
        () ->
            with(of(values)).map(Pipe.pipe(map((Integer v) -> v + 1)).then(map(v -> v - 1))).get());
  }
}
