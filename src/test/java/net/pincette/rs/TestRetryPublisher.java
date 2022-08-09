package net.pincette.rs;

import static java.time.Duration.ofSeconds;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.Util.retryPublisher;
import static net.pincette.util.Collections.list;

import net.pincette.util.State;
import net.pincette.util.Util.GeneralException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestRetryPublisher {
  @Test
  @DisplayName("retry publisher")
  void test() {
    final State<Integer> state = new State<>();

    runTest(
        list(1, 2, 1, 2, 3),
        () -> {
          state.set(0);

          return retryPublisher(
              () ->
                  with(of(list(1, 2, 3)))
                      .map(
                          v -> {
                            state.set(state.get() + 1);

                            if (state.get() == 3) {
                              throw new GeneralException("test");
                            }

                            return v;
                          })
                      .get(),
              ofSeconds(1));
        },
        1);
  }
}
