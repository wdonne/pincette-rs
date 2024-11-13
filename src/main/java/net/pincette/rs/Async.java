package net.pincette.rs;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Mapper.map;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.function.Function;

/**
 * Emits the values produced by the stages in the order the stages arrive. The stream completes only
 * after the last stage has completed.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 1.5
 */
public class Async<T> extends ProcessorBase<CompletionStage<T>, T> {
  private CompletionStage<Void> previous = completedFuture(null);

  public static <T> Processor<CompletionStage<T>, T> async() {
    return new Async<>();
  }

  /**
   * Returns a processor with the mapping function, which transforms the objects. The completion
   * stages are processed in the order of the stream, which completes only after the last stage is
   * completed. This means the functions may start in parallel, but the completions are emitted in
   * the proper order.
   *
   * @param function the mapping function.
   * @param <T> the incoming value type.
   * @param <R> the outgoing value type.
   * @return The processor.
   * @since 3.0
   */
  public static <T, R> Processor<T, R> mapAsync(final Function<T, CompletionStage<R>> function) {
    return box(map(function), async());
  }

  /**
   * Returns a processor with the mapping function, which transforms the objects. The completion
   * stages are processed in the order of the stream, which completes only after the last stage is
   * completed. The functions are executed in sequence, which means a function call starts only
   * after the previous completion stage has completed.
   *
   * @param function the mapping function.
   * @param <T> the incoming value type.
   * @param <R> the outgoing value type.
   * @return The processor.
   * @since 3.1.2
   */
  public static <T, R> Processor<T, R> mapAsyncSequential(
      final Function<T, CompletionStage<R>> function) {
    return AsyncDepend.mapAsync((v, p) -> function.apply(v));
  }

  @Override
  protected void emit(final long number) {
    subscription.request(number);
  }

  @Override
  public void onComplete() {
    dispatch(
        () -> {
          if (!getError()) {
            previous.thenRunAsync(() -> subscriber.onComplete());
          }
        });
  }

  public void onNext(final CompletionStage<T> stage) {
    if (stage == null) {
      throw new NullPointerException("Can't emit null.");
    }

    dispatch(
        () -> {
          if (!getError()) {
            previous =
                previous
                    .thenComposeAsync(v -> stage.thenAccept(value -> subscriber.onNext(value)))
                    .exceptionally(
                        t -> {
                          setError(true);
                          subscriber.onError(t);
                          subscription.cancel();

                          return null;
                        });
          }
        });
  }
}
