package net.pincette.rs;

import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.rs.Source.of;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;

/**
 * Some utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Util {
  private Util() {}

  @SuppressWarnings("java:S1905") // For type inference.
  private static <T> CompletionStage<Optional<T>> element(
      final Publisher<T> publisher, final Processor<T, T> terminator) {
    return reduce(subscribe(publisher, terminator), () -> (T) null, (result, value) -> value)
        .thenApply(Optional::ofNullable);
  }

  /**
   * Returns a publisher that emits no values.
   *
   * @param <T> the value type.
   * @return The empty publisher.
   * @since 1.0
   */
  public static <T> Publisher<T> empty() {
    return of(new ArrayList<>());
  }

  /**
   * Returns the first element <code>publisher</code> emits if there is one.
   *
   * @param publisher the given publisher.
   * @param <T> the value type.
   * @return The optional first element.
   * @since 1.4
   */
  public static <T> CompletionStage<Optional<T>> first(final Publisher<T> publisher) {
    return element(publisher, new First<>());
  }

  /**
   * Returns a blocking <code>Iterable</code> with a request size of 100.
   *
   * @param publisher the publisher from which the elements are buffered.
   * @param <T> the element type.
   * @return The iterable.
   * @since 1.2
   */
  public static <T> Iterable<T> iterate(final Publisher<T> publisher) {
    return iterate(publisher, 100);
  }

  /**
   * Returns a blocking <code>Iterable</code>.
   *
   * @param publisher the publisher from which the elements are buffered.
   * @param requestSize the size of the requests the subscriber will issue to the publisher.
   * @param <T> the element type.
   * @return The iterable.
   * @since 1.2
   */
  public static <T> Iterable<T> iterate(final Publisher<T> publisher, final long requestSize) {
    final BlockingSubscriber<T> subscriber = new BlockingSubscriber<>(requestSize);

    publisher.subscribe(subscriber);

    return subscriber;
  }

  /**
   * Blocks until the publisher is done.
   *
   * @param publisher the given publisher.
   * @param <T> the value type.
   * @since 1.5
   */
  public static <T> void join(final Publisher<T> publisher) {
    reduce(publisher, (v1, v2) -> v1).toCompletableFuture().join();
  }

  /**
   * Returns the last element <code>publisher</code> emits if there is one.
   *
   * @param publisher the given publisher.
   * @param <T> the value type.
   * @return The optional first element.
   * @since 1.4
   */
  public static <T> CompletionStage<Optional<T>> last(final Publisher<T> publisher) {
    return element(publisher, new Last<>());
  }

  static void parking(final Object blocker, final long timeout) {
    if (timeout != -1) {
      parkNanos(blocker, timeout * 1000);
    } else {
      park(blocker);
    }
  }

  /**
   * Subscribes <code>processor</code> to <code>publisher</code> and returns the processor as a
   * publisher.
   *
   * @param publisher the given publisher.
   * @param processor the given processor.
   * @param <T> the type of the publisher.
   * @param <R> the type of the returned publisher.
   * @return The processor as a publisher.
   * @since 1.0
   */
  public static <T, R> Publisher<R> subscribe(
      final Publisher<T> publisher, final Processor<T, R> processor) {
    publisher.subscribe(processor);

    return processor;
  }
}
