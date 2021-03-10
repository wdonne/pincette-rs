package net.pincette.rs;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.rs.Source.of;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import net.pincette.util.State;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Some utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Util {
  private Util() {}

  /**
   * Returns the published values as a list. This function will block when needed.
   *
   * @param publisher the publisher.
   * @param <T> the value type.
   * @return The generated list.
   * @since 1.6
   */
  public static <T> List<T> asList(final Publisher<T> publisher) {
    return asListAsync(publisher).toCompletableFuture().join();
  }

  public static <T> CompletionStage<List<T>> asListAsync(final Publisher<T> publisher) {
    final CompletableFuture<List<T>> future = new CompletableFuture<>();

    publisher.subscribe(completerList(future));

    return future;
  }

  private static <T> LambdaSubscriber<T> completerList(final CompletableFuture<List<T>> future) {
    final List<T> result = new ArrayList<>();

    return new LambdaSubscriber<>(
        result::add, () -> future.complete(result), future::completeExceptionally);
  }

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
    return of(emptyList());
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
   * Returns a publisher that always emits values from <code>supplier</code> when asked for
   * messages.
   *
   * @param supplier the generator function.
   * @param <T> the value type.
   * @return The publisher.
   * @since 1.6
   */
  public static <T> Publisher<T> generate(final Supplier<T> supplier) {
    return subscriber ->
        subscriber.onSubscribe(
            new Subscription() {

              @Override
              public void cancel() {
                // Nothing to do.
              }

              @Override
              public void request(final long n) {
                for (long i = 0; i < n; ++i) {
                  subscriber.onNext(supplier.get());
                }
              }
            });
  }

  /**
   * Returns a publisher that always emits values from <code>initial</code> and <code>next</code>
   * when asked for messages.
   *
   * @param initial the generator function for the first value.
   * @param next the generator function for all the subsequent values. The function receives the
   *     last value that was emitted.
   * @param <T> the value type.
   * @return The publisher.
   * @since 1.6
   */
  public static <T> Publisher<T> generate(final Supplier<T> initial, final UnaryOperator<T> next) {
    final State<T> state = new State<>();

    return generate(() -> state.set(state.get() == null ? initial.get() : next.apply(state.get())));
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
   * Returns a publisher that will recreate the original publisher when an exception occurs.
   *
   * @param publisher the function to create the original publisher.
   * @param retryInterval the time before a new attempt is made.
   * @param <T> the value type.
   * @return The retrying publisher.
   * @since 1.6
   */
  public static <T> Publisher<T> retryPublisher(
      final Supplier<Publisher<T>> publisher, final Duration retryInterval) {
    return retryPublisher(publisher, retryInterval, null);
  }

  /**
   * Returns a publisher that will recreate the original publisher when an exception occurs.
   *
   * @param publisher the function to create the original publisher.
   * @param retryInterval the time before a new attempt is made.
   * @param onException the handler of the exception. It may be <code>null</code>.
   * @param <T> the value type.
   * @return The retrying publisher.
   * @since 1.6
   */
  public static <T> Publisher<T> retryPublisher(
      final Supplier<Publisher<T>> publisher,
      final Duration retryInterval,
      final Consumer<Throwable> onException) {
    final Retry<T> result = new Retry<>(publisher, retryInterval, onException);

    publisher.get().subscribe(result);

    return result;
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

  /**
   * Subscribes <code>subscriber</code> to <code>processor</code> and returns <code>processor</code>
   * as a subscriber.
   *
   * @param processor the given processor.
   * @param subscriber the given subscriber.
   * @param <T> the input type of the processor.
   * @param <R> the output type of the processor.
   * @return The processor as a subscriber.
   * @since 1.6
   */
  public static <T, R> Subscriber<T> subscribe(
      final Processor<T, R> processor, final Subscriber<R> subscriber) {
    processor.subscribe(subscriber);

    return processor;
  }

  private static class Retry<T> extends PassThrough<T> {
    private final Consumer<Throwable> onException;
    private final Supplier<Publisher<T>> publisher;
    private final Duration retryInterval;
    private Subscriber<? super T> subscriber;

    private Retry(
        final Supplier<Publisher<T>> publisher,
        final Duration retryInterval,
        final Consumer<Throwable> onException) {
      this.publisher = publisher;
      this.retryInterval = retryInterval;
      this.onException = onException;
    }

    @Override
    public void onError(Throwable t) {
      setError(true);

      if (onException != null) {
        onException.accept(t);
      }

      composeAsyncAfter(() -> completedFuture(publisher.get()), retryInterval)
          .thenAccept(
              p -> {
                setError(false);
                p.subscribe(this);
                this.subscribe(subscriber);
              });
    }

    @Override
    public void subscribe(final Subscriber<? super T> subscriber) {
      super.subscribe(subscriber);
      this.subscriber = subscriber;
    }
  }
}
