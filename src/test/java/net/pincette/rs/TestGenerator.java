package net.pincette.rs;

import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.Util.generate;
import static net.pincette.util.Collections.list;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestGenerator {
  @Test
  @DisplayName("generator")
  void generator() {
    runTest(list(0, 1, 2, 3), () -> generate(() -> 0, v -> v < 3 ? (v + 1) : null));
  }
}
