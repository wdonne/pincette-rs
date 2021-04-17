package net.pincette.rs;

import static net.pincette.util.Util.rethrow;
import static net.pincette.util.Util.tryToDo;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

/**
 * Functions to reduce a publisher and return the result as a {@link
 * java.util.concurrent.CompletionStage}.
 *
 * @author Werner Donné
 * @since 1.1
 */
public class Reducer {
  private Reducer() {}

  /**
   * Runs <code>consumer</code> for each emitted value.
   *
   * @param publisher the given publisher.
   * @param consumer the consumer function.
   * @param <T> the value type of the publisher.
   * @return The completion stage.
   * @since 1.4.1
   */
  public static <T> CompletionStage<Void> forEach(
      final Publisher<T> publisher, final Consumer<T> consumer) {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    publisher.subscribe(
        new LambdaSubscriber<>(
            consumer::accept, () -> future.complete(null), future::completeExceptionally));

    return future;
  }

  /**
   * Runs <code>consumer</code> for each emitted value and makes it synchronous.
   *
   * @param publisher the given publisher.
   * @param consumer the consumer function.
   * @param <T> the value type of the publisher.
   * @since 1.4.1
   */
  public static <T> void forEachJoin(final Publisher<T> publisher, final Consumer<T> consumer) {
    forEach(publisher, consumer).toCompletableFuture().join();
  }

  /**
   * Accumulates all the values emitted by the publisher into a new value.
   *
   * @param publisher the given publisher.
   * @param identity the function to produce the initial accumulated value.
   * @param accumulator the function to accumulate all the values.
   * @param <T> the value type of the publisher.
   * @param <U> the value type of the result.
   * @return The completion stage with the result.
   * @since 1.1
   */
  public static <T, U> CompletionStage<U> reduce(
      final Publisher<T> publisher,
      final Supplier<U> identity,
      final BiFunction<U, T, U> accumulator) {
    final CompletableFuture<U> future = new CompletableFuture<>();
    final State<U> state = new State<>(identity.get());

    publisher.subscribe(
        new LambdaSubscriber<>(
            value ->
                tryToDo(
                    () -> state.set(accumulator.apply(state.value, value)),
                    e -> {
                      future.completeExceptionally(e);
                      rethrow(e);
                    }),
            () -> future.complete(state.value),
            future::completeExceptionally));

    return future;
  }

  /**
   * Accumulates all the values emitted by the publisher by combining them.
   *
   * @param publisher the given publisher.
   * @param accumulator the associative function that combines the values.
   * @param <T> the value type.
   * @return The completion stage with the result. The optional will be empty when the publisher
   *     didn't emit any values before completing.
   * @since 1.1
   */
  public static <T> CompletionStage<Optional<T>> reduce(
      final Publisher<T> publisher, final BinaryOperator<T> accumulator) {
    return reduce(
            publisher,
            () -> new State<T>(null),
            (state, value) ->
                state.set(state.value != null ? accumulator.apply(state.value, value) : value))
        .thenApply(result -> Optional.ofNullable(result.value));
  }

  /**
   * Accumulates all the values emitted by the publisher into a new value and makes it synchronous.
   *
   * @param publisher the given publisher.
   * @param identity the function to produce the initial accumulated value.
   * @param accumulator the function to accumulate all the values.
   * @param <T> the value type of the publisher.
   * @param <U> the value type of the result.
   * @return The completion stage with the result.
   * @since 1.4.1
   */
  public static <T, U> U reduceJoin(
      final Publisher<T> publisher,
      final Supplier<U> identity,
      final BiFunction<U, T, U> accumulator) {
    return reduce(publisher, identity, accumulator).toCompletableFuture().join();
  }

  /**
   * Accumulates all the values emitted by the publisher by combining them and makes it synchronous.
   *
   * @param publisher the given publisher.
   * @param accumulator the associative function that combines the values.
   * @param <T> the value type.
   * @return The completion stage with the result. The optional will be empty when the publisher
   *     didn't emit any values before completing.
   * @since 1.4.1
   */
  public static <T> Optional<T> reduceJoin(
      final Publisher<T> publisher, final BinaryOperator<T> accumulator) {
    return reduce(publisher, accumulator).toCompletableFuture().join();
  }

  private static class State<T> {
    private T value;

    private State(final T value) {
      this.value = value;
    }

    private State<T> set(final T value) {
      this.value = value;

      return this;
    }
  }
}
