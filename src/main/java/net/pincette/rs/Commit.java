package net.pincette.rs;

import static java.lang.Boolean.TRUE;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;

/**
 * When the down stream requests more messages this indicates all messages it has received were
 * processed correctly. This is a moment to perform a commit with a function that receives the list
 * of uncommitted messages. This supports at least once semantics.
 *
 * @param <T> the value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class Commit<T> extends ProcessorBase<T, T> {
  private final Function<List<T>, CompletionStage<Boolean>> fn;
  private final Deque<T> uncommitted = new ConcurrentLinkedDeque<>();
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

  @Override
  protected void emit(final long number) {
    last()
        .map(fn)
        .orElseGet(() -> completedFuture(true))
        .thenAccept(
            result -> {
              if (TRUE.equals(result)) {
                request(number);
              }
            });
  }

  private Optional<List<T>> last() {
    final List<T> result = new ArrayList<>();

    while (!uncommitted.isEmpty() && requested-- > 0) {
      result.add(uncommitted.removeLast());
    }

    return Optional.of(result).filter(r -> !r.isEmpty());
  }

  @Override
  public void onComplete() {
    super.onComplete();
    last().ifPresent(fn::apply);
  }

  @Override
  public void onNext(final T value) {
    uncommitted.addFirst(value);
    subscriber.onNext(value);
  }

  private void request(final long number) {
    requested = number;
    subscription.request(number);
  }
}