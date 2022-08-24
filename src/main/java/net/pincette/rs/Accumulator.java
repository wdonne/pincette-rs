package net.pincette.rs;

import static net.pincette.util.Util.rethrow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * Accumulates a publisher and when that's done it calls the provided function. With the <code>get
 * </code> method you obtain the final result.
 *
 * @author Werner Donn\u00e9
 * @since 1.3
 */
public class Accumulator<T, U> implements Subscriber<T> {
  private final UnaryOperator<T> copy;
  private final CompletableFuture<U> future = new CompletableFuture<>();
  private final List<T> list = new ArrayList<>();
  private final Function<Stream<T>, CompletionStage<U>> reducer;
  private Subscription subscription;

  /**
   * Constructs the accumulator with a reducer.
   *
   * @param reducer the reducer function.
   * @since 1.3
   */
  public Accumulator(final Function<Stream<T>, CompletionStage<U>> reducer) {
    this(reducer, null);
  }

  /**
   * Constructs the accumulator with a reducer.
   *
   * @param reducer the reducer function.
   * @param copy the function that copies the values before they are accumulated. It may be <code>
   *     null</code>.
   * @since 3.0.1
   */
  public Accumulator(
      final Function<Stream<T>, CompletionStage<U>> reducer, final UnaryOperator<T> copy) {
    this.reducer = reducer;
    this.copy = copy;
  }

  public static <T, U> Subscriber<T> accumulator(
      final Function<Stream<T>, CompletionStage<U>> reducer) {
    return new Accumulator<>(reducer);
  }

  public static <T, U> Subscriber<T> accumulator(
      final Function<Stream<T>, CompletionStage<U>> reducer, final UnaryOperator<T> copy) {
    return new Accumulator<>(reducer, copy);
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

  private void more() {
    subscription.request(1);
  }

  public void onComplete() {
    reducer.apply(list.stream()).thenAccept(future::complete);
  }

  public void onError(final Throwable t) {
    if (t == null) {
      throw new NullPointerException("Can't throw null.");
    }

    rethrow(t);
  }

  public void onNext(final T value) {
    if (value == null) {
      throw new NullPointerException("Can't emit null.");
    }

    list.add(copy != null ? copy.apply(value) : value);
    more();
  }

  public void onSubscribe(final Subscription subscription) {
    if (subscription == null) {
      throw new NullPointerException("A subscription can't be null.");
    }

    if (this.subscription != null) {
      subscription.cancel();
    } else {
      this.subscription = subscription;
      more();
    }
  }
}
