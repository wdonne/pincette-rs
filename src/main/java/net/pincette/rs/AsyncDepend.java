package net.pincette.rs;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.util.Collections.list;

import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Flow.Processor;
import java.util.function.BiFunction;
import net.pincette.function.SideEffect;

/**
 * Emits the values produced by functions in the order the values arrive. The functions also receive
 * the result of the previous call, or <code>null</code> if it is the first call. The stream
 * completes only after the last stage has completed.
 *
 * @param <T> the incoming value type.
 * @param <R> the outgoing value type.
 * @author Werner Donn\u00e9
 * @since 3.0
 */
public class AsyncDepend<T, R> extends ProcessorBase<T, R> {
  private final BiFunction<T, R, CompletionStage<R>> function;
  private final Deque<CompletionStage<R>> stages =
      new ConcurrentLinkedDeque<>(list(completedFuture(null)));

  public AsyncDepend(final BiFunction<T, R, CompletionStage<R>> function) {
    this.function = function;
  }

  /**
   * Returns a processor with the mapping function, which transforms the objects. The functions
   * stages are executed in the order of the stream, which completes only after the last stage is
   * completed. A function call will also receive the result of the previous call, which is <code>
   * null</code> for the first call.
   *
   * @param function the mapping function.
   * @param <T> the incoming value type.
   * @param <R> the outgoing value type.
   * @return The processor.
   */
  public static <T, R> Processor<T, R> mapAsync(
      final BiFunction<T, R, CompletionStage<R>> function) {
    return new AsyncDepend<>(function);
  }

  @Override
  protected void emit(final long number) {
    subscription.request(number);
  }

  @Override
  public void onComplete() {
    if (!getError()) {
      stages.getFirst().thenRunAsync(() -> subscriber.onComplete());
    }
  }

  @Override
  public void onNext(final T value) {
    if (value == null) {
      throw new NullPointerException("Can't emit null.");
    }

    if (!getError()) {
      final CompletionStage<R> previous = stages.getFirst();
      final CompletableFuture<R> next = new CompletableFuture<>();

      stages.addFirst(next);

      previous
          .thenComposeAsync(v -> function.apply(value, v))
          .thenApply(
              r ->
                  SideEffect.<R>run(
                          () -> {
                            subscriber.onNext(r);
                            next.complete(r);
                          })
                      .andThenGet(() -> r))
          .exceptionally(
              t -> {
                subscriber.onError(t);
                subscription.cancel();

                return null;
              });

      while (stages.size() > 10) {
        stages.removeLast();
      }
    }
  }
}
