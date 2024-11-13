package net.pincette.rs;

import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.Util.carryOver;
import static net.pincette.util.Collections.list;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestCarryOver {
  @Test
  @DisplayName("carryOver")
  void test() {
    runTest(
        list(3, 6, 9, 12),
        () ->
            with(Source.of(1, 2, 3, 4))
                .map(carryOver(map(v -> v), v -> 2 * v, Integer::sum))
                .get());
  }
}
