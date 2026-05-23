package net.pincette.rs;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.Source.of;
import static net.pincette.rs.TestUtil.runTest;
import static net.pincette.rs.Util.retryPublisher;
import static net.pincette.util.Collections.list;
import static net.pincette.util.ScheduledCompletionStage.runAsyncAfter;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import net.pincette.util.State;
import net.pincette.util.Util.GeneralException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestRetryPublisher {
  @Test
  @DisplayName("retry publisher 1")
  void retry1() {
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
              ofMillis(100));
        },
        100);
  }

  @Test
  @DisplayName("retry publisher 2")
  void retry2() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    final State<Boolean> started = new State<>(true);

    retryPublisher(
            () -> {
              started.set(true);
              return with(of(list(1)))
                  .map(
                      v -> {
                        throw new GeneralException("test");
                      })
                  .get();
            },
            ofMillis(100),
            t -> started.set(false))
        .subscribe(
            lambdaSubscriber(
                v -> {},
                () -> {},
                t -> {},
                subscription ->
                    runAsyncAfter(subscription::cancel, ofSeconds(5))
                        .thenRun(() -> future.complete(null))));

    future.join();
    assertFalse(started.get());
  }
}
