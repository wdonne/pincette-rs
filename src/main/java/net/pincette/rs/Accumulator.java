package net.pincette.rs;

import static net.pincette.util.Util.rethrow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Accumulates a publisher and when that's done it calls the provided function. With the <code>get
 * </code> method you obtain the final result.
 *
 * @author Werner Donn\u00e0
 * @since 1.3
 */
public class Accumulator<T, U> implements Subscriber<T> {
  private final List<T> list = new ArrayList<>();
  private final CompletableFuture<U> future = new CompletableFuture<>();
  private final Function<Stream<T>, CompletionStage<U>> reducer;
  private Subscription subscription;

  /**
   * Constructs the accumulator with a reducer.
   *
   * @param reducer the reducer function.
   * @since 1.3
   */
  public Accumulator(final Function<Stream<T>, CompletionStage<U>> reducer) {
    this.reducer = reducer;
  }

  /**
   * Returns the reduced value when the stage is complete.
   *
   * @return The stage to received the reduced value.
   * @since 1.3
   */
  public CompletionStage<U> get() {
    return future;
  }

  public void onComplete() {
    reducer.apply(list.stream()).thenAccept(future::complete);
  }

  public void onError(final Throwable t) {
    rethrow(t);
  }

  public void onNext(final T value) {
    list.add(value);
    subscription.request(1);
  }

  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;
    subscription.request(1);
  }
}
