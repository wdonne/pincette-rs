package net.pincette.rs;

import static java.lang.Boolean.TRUE;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.rs.Util.throwBackpressureViolation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;

/**
 * When the down stream requests more messages the stream this indicates all messages it has
 * received were processed correctly. This is a moment to perform a commit with a function that
 * receives the list of uncommitted messages. This supports at least once semantics.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.0
 */
public class Commit<T> extends ProcessorBase<T, T> {
  private final Function<List<T>, CompletionStage<Boolean>> fn;
  private final Deque<T> uncommitted = new ArrayDeque<>(1000);
  private boolean completed;
  private long requested;

  /**
   * Create a Commit processor.
   *
   * @param commit the commit function. New messages are only requested when the completion stage
   *     returns <code>true</code>.
   */
  public Commit(final Function<List<T>, CompletionStage<Boolean>> commit) {
    this.fn = commit;
  }

  public static <T> Processor<T, T> commit(
      final Function<List<T>, CompletionStage<Boolean>> commit) {
    return new Commit<>(commit);
  }

  private CompletionStage<Boolean> commit() {
    return last().map(fn).orElseGet(() -> completedFuture(true));
  }

  private void commit(final Runnable run) {
    commit()
        .thenAccept(
            result -> {
              if (TRUE.equals(result)) {
                run.run();
              }
            });
  }

  @Override
  protected void emit(final long number) {
    dispatch(
        () ->
            commit(
                () ->
                    dispatch(
                        () -> {
                          requested += number;

                          if (!completed) {
                            subscription.request(number);
                          } else {
                            sendComplete();
                          }
                        })));
  }

  private Optional<List<T>> last() {
    final List<T> result = new ArrayList<>();

    while (!uncommitted.isEmpty()) {
      result.add(uncommitted.removeLast());
    }

    return Optional.of(result).filter(r -> !r.isEmpty());
  }

  @Override
  public void onComplete() {
    dispatch(
        () -> {
          completed = true;

          if (requested > 0) {
            sendComplete();
          }
        });
  }

  @Override
  public void onNext(final T value) {
    dispatch(
        () -> {
          if (requested == 0) {
            throwBackpressureViolation(this, subscription, requested);
          }

          uncommitted.addFirst(value);
          --requested;
          subscriber.onNext(value);
        });
  }

  private void sendComplete() {
    if (uncommitted.isEmpty()) {
      super.onComplete();
    }
  }
}
