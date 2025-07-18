package net.pincette.rs;

import static java.lang.Math.min;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.logging.Logger.getLogger;
import static net.pincette.rs.Async.mapAsyncSequential;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Buffer.buffer;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Combine.combine;
import static net.pincette.rs.Encode.encode;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.NotFilter.notFilter;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Probe.probe;
import static net.pincette.rs.Probe.probeCancel;
import static net.pincette.rs.Probe.probeComplete;
import static net.pincette.rs.Reducer.reduce;
import static net.pincette.rs.Source.of;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.ScheduledCompletionStage.supplyAsyncAfter;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.Util.tryToDoRethrow;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import net.pincette.function.ConsumerWithException;
import net.pincette.function.RunnableWithException;
import net.pincette.function.SideEffect;
import net.pincette.rs.encoders.Deflate;
import net.pincette.rs.encoders.DivisibleBy;
import net.pincette.rs.encoders.Gunzip;
import net.pincette.rs.encoders.Gzip;
import net.pincette.rs.encoders.Inflate;
import net.pincette.rs.encoders.Lines;
import net.pincette.util.State;
import net.pincette.util.TimedCache;
import net.pincette.util.Util.GeneralException;

/**
 * Some utilities.
 *
 * @author Werner Donné
 * @since 1.0
 */
public class Util {
  static final Logger LOGGER = getLogger("net.pincette.rs.base");

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

  public static <T> List<T> asList(final Publisher<T> publisher, final int initialCapacity) {
    return asListAsync(publisher, initialCapacity).toCompletableFuture().join();
  }

  /**
   * Returns the published values as a list.
   *
   * @param publisher the publisher.
   * @param <T> the value type.
   * @return The generated list.
   * @since 1.6
   */
  public static <T> CompletionStage<List<T>> asListAsync(final Publisher<T> publisher) {
    return asListAsync(publisher, 1000);
  }

  public static <T> CompletionStage<List<T>> asListAsync(
      final Publisher<T> publisher, final int initialCapacity) {
    final CompletableFuture<List<T>> future = new CompletableFuture<>();

    publisher.subscribe(completerList(future, initialCapacity));

    return future;
  }

  /**
   * Returns the first value. This function will block when needed.
   *
   * @param publisher the publisher.
   * @param <T> the value type.
   * @return The value.
   * @since 1.7.1
   */
  public static <T> T asValue(final Publisher<T> publisher) {
    return asValueAsync(publisher).toCompletableFuture().join();
  }

  /**
   * Completes with the first emitted value or <code>null</code> if there was no such value.
   *
   * @param publisher the publisher.
   * @param <T> the value type.
   * @return The value.
   * @since 1.7.1
   */
  public static <T> CompletionStage<T> asValueAsync(final Publisher<T> publisher) {
    final CompletableFuture<T> future = new CompletableFuture<>();

    with(publisher).first().get().subscribe(completerFirst(future));

    return future;
  }

  /**
   * This is a debugging aid that throws an exception when more messages are sent than requested.
   *
   * @return The pass through processor.
   * @param <T> the value type.
   * @since 3.0.2
   */
  public static <T> Processor<T, T> backpressureCheck() {
    final State<Long> requested = new State<>(0L);
    final State<Long> sent = new State<>(0L);

    return probe(
        n -> requested.set(requested.get() + n),
        v -> {
          sent.set(sent.get() + 1);

          if (sent.get() > requested.get()) {
            throw new GeneralException(
                "Sending "
                    + sent.get()
                    + " values, while only "
                    + requested.get()
                    + " have been requested.");
          }
        });
  }

  /**
   * Subscribes to the publisher and cancels it immediately.
   *
   * @param publisher the given publisher.
   * @param <T> the value type.
   * @since 3.4
   */
  public static <T> void cancel(final Publisher<T> publisher) {
    publisher.subscribe(lambdaSubscriber(v -> {}, () -> {}, t -> {}, Subscription::cancel));
  }

  /**
   * With this function you can hold on to a value before the messages goes through the wrapped
   * processor because it won't be available anymore in the message that comes out of it.
   *
   * @param wrapped the processor over which a value is carried.
   * @param split the function that calculates the value that is to be carried over the wrapped
   *     processor.
   * @param merge the function that receives the messages from the wrapped processor and the value
   *     that is carried over. If the wrapped processor produces more than one message for an
   *     incoming message, the function will be called for all those messages.
   * @return The new processor.
   * @param <T> the input type of the processor.
   * @param <R> the output type of the processor.
   * @param <S> the type of the value that is carried over.
   * @since 3.7.0
   */
  public static <T, R, S> Processor<T, R> carryOver(
      final Processor<T, R> wrapped, final Function<T, S> split, final BiFunction<R, S, R> merge) {
    final State<S> value = new State<>(null);

    return pipe(map(
            (T v) -> {
              value.set(split.apply(v));
              return v;
            }))
        .then(wrapped)
        .then(map(v -> merge.apply(v, value.get())));
  }

  public static <T> Publisher<T> completablePublisher(
      final Supplier<CompletionStage<Publisher<T>>> publisher) {
    final Processor<T, T> passThrough = passThrough();

    publisher.get().thenAccept(p -> p.subscribe(passThrough));

    return passThrough;
  }

  private static Subscriber<Void> completerEmpty(final CompletableFuture<Void> future) {
    return lambdaSubscriber(v -> {}, () -> future.complete(null), future::completeExceptionally);
  }

  private static <T> Subscriber<T> completerFirst(final CompletableFuture<T> future) {
    return lambdaSubscriber(
        future::complete, () -> future.complete(null), future::completeExceptionally);
  }

  private static <T> Subscriber<T> completerList(
      final CompletableFuture<List<T>> future, final int initialCapacity) {
    final List<T> result = new ArrayList<>(initialCapacity);

    return lambdaSubscriber(
        result::add, () -> future.complete(result), future::completeExceptionally);
  }

  static <T> List<T> consume(final Deque<T> deque, final long requested) {
    return rangeExclusive(0, min(deque.size(), requested)).map(i -> deque.pollLast()).toList();
  }

  /**
   * Creates a processor that deflates a <code>ByteBuffer</code> stream.
   *
   * @return The processor.
   * @since 3.0
   */
  public static Processor<ByteBuffer, ByteBuffer> deflate() {
    return encode(Deflate.deflate());
  }

  /**
   * Creates a processor that deflates a <code>ByteBuffer</code> stream.
   *
   * @return The processor.
   * @since 3.0
   */
  public static Processor<ByteBuffer, ByteBuffer> deflate(final Deflater deflater) {
    return encode(Deflate.deflate(deflater));
  }

  public static <T> Subscriber<T> devNull() {
    return lambdaSubscriber(v -> {}, () -> {}, t -> {});
  }

  /**
   * Consumes the publisher without doing anything.
   *
   * @param publisher the given publisher.
   * @param <T> the value type.
   * @since 3.0
   */
  public static <T> void discard(final Publisher<T> publisher) {
    publisher.subscribe(devNull());
  }

  /**
   * Returns a processor that produces byte buffers the size of which is divisible by a certain
   * number, where the last one may be smaller than that number.
   *
   * @since 3.0
   */
  public static Processor<ByteBuffer, ByteBuffer> divisibleBy(final int n) {
    return encode(DivisibleBy.divisibleBy(n));
  }

  /**
   * Filters the elements leaving out duplicates according to the <code>criterion</code> over the
   * time <code>window</code>.
   *
   * @param criterion the given criterion.
   * @param window the time window to look at.
   * @param <T> the value type.
   * @param <U> the criterion type.
   * @return The filtered stream.
   * @since 3.0
   */
  public static <T, U> Processor<T, T> duplicateFilter(
      final Function<T, U> criterion, final Duration window) {
    final TimedCache<U, U> cache = new TimedCache<>(window);

    return pipe(map((T v) -> pair(v, criterion.apply(v))))
        .then(notFilter(pair -> cache.get(pair.second).isPresent()))
        .then(
            map(
                pair ->
                    Optional.of(pair.second)
                        .map(
                            c ->
                                SideEffect.<T>run(() -> cache.put(c, c))
                                    .andThenGet(() -> pair.first))
                        .orElse(null)));
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
   * Waits until the empty publisher completes.
   *
   * @param publisher the publisher.
   * @since 1.7.1
   */
  public static void empty(final Publisher<Void> publisher) {
    emptyAsync(publisher).toCompletableFuture().join();
  }

  /**
   * Completes when the empty publisher completes.
   *
   * @param publisher the publisher.
   * @return The empty completion stage.
   * @since 1.7.1
   */
  public static CompletionStage<Void> emptyAsync(final Publisher<Void> publisher) {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    publisher.subscribe(completerEmpty(future));

    return future;
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
   * messages. When the supplier returns <code>null</code> the publisher completes.
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
              public void cancel() {
                subscriber.onComplete();
              }

              public void request(final long n) {
                boolean completed = false;

                for (long i = 0; !completed && i < n; ++i) {
                  final T value = supplier.get();

                  if (value != null) {
                    subscriber.onNext(value);
                  } else {
                    completed = true;
                    subscriber.onComplete();
                  }
                }
              }
            });
  }

  /**
   * Returns a publisher that always emits values from <code>initial</code> and <code>next</code>
   * when asked for messages. When the supplier returns <code>null</code> the publisher completes.
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
   * Creates a processor that compresses a <code>ByteBuffer</code> stream in GZIP format.
   *
   * @return The processor.
   * @since 3.0
   */
  public static Processor<ByteBuffer, ByteBuffer> gzip() {
    return encode(Gzip.gzip());
  }

  /**
   * Creates a processor that uncompresses a <code>ByteBuffer</code> stream in GZIP format.
   *
   * @return The processor.
   * @since 3.0
   */
  public static Processor<ByteBuffer, ByteBuffer> gunzip() {
    return encode(Gunzip.gunzip());
  }

  /**
   * Creates a processor that inflates a <code>ByteBuffer</code> stream.
   *
   * @return The processor.
   * @since 3.0
   */
  public static Processor<ByteBuffer, ByteBuffer> inflate() {
    return encode(Inflate.inflate());
  }

  /**
   * Creates a processor that inflates a <code>ByteBuffer</code> stream.
   *
   * @return The processor.
   * @since 3.0
   */
  public static Processor<ByteBuffer, ByteBuffer> inflate(final Inflater inflater) {
    return encode(Inflate.inflate(inflater));
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

  /**
   * Returns a processor that receives buffers and interprets the contents as a UTF-8 encoded
   * string. It emits the individual lines in the string without the line separators.
   *
   * @since 3.0
   */
  public static Processor<ByteBuffer, String> lines() {
    return encode(Lines.lines());
  }

  static <T> Optional<List<T>> nextValues(final Deque<T> deque, final long amount) {
    return Optional.of(deque).filter(b -> !b.isEmpty() && amount > 0).map(b -> consume(b, amount));
  }

  /**
   * Returns a processor that reacts to the cancellation of the subscription. It is transparent to
   * all events.
   *
   * @param runnable the given function.
   * @param <T> the value type.
   * @return The processor.
   * @since 3.6.2
   */
  public static <T> Processor<T, T> onCancelProcessor(final RunnableWithException runnable) {
    return probeCancel(() -> tryToDoRethrow(runnable));
  }

  /**
   * Returns a subscriber that reacts to the <code>onComplete</code> event.
   *
   * @param runnable the given function.
   * @param <T> the value type.
   * @return The subscriber.
   * @since 3.0
   */
  public static <T> Subscriber<T> onComplete(final RunnableWithException runnable) {
    return lambdaSubscriber(v -> {}, runnable);
  }

  /**
   * Returns a processor that reacts to the <code>onComplete</code> event. It is transparent to all
   * events.
   *
   * @param runnable the given function.
   * @param <T> the value type.
   * @return The processor.
   * @since 3.2.2
   */
  public static <T> Processor<T, T> onCompleteProcessor(final RunnableWithException runnable) {
    return probeComplete(() -> tryToDoRethrow(runnable));
  }

  /**
   * Returns a subscriber that reacts to the <code>onError</code> event.
   *
   * @param consumer the given function.
   * @param <T> the value type.
   * @return The subscriber.
   * @since 3.0
   */
  public static <T> Subscriber<T> onError(final ConsumerWithException<Throwable> consumer) {
    return lambdaSubscriber(v -> {}, () -> {}, consumer);
  }

  /**
   * Returns a processor that reacts to the <code>onError</code> event. It is transparent to all
   * events.
   *
   * @param consumer the given function.
   * @param <T> the value type.
   * @return The processor.
   * @since 3.2.2
   */
  public static <T> Processor<T, T> onErrorProcessor(
      final ConsumerWithException<Throwable> consumer) {
    return probe(n -> {}, v -> {}, () -> {}, t -> tryToDoRethrow(() -> consumer.accept(t)));
  }

  /**
   * Returns a subscriber that reacts to the <code>onNext</code> event.
   *
   * @param consumer the given function.
   * @param <T> the value type.
   * @return The subscriber.
   * @since 3.0
   */
  public static <T> Subscriber<T> onNext(final ConsumerWithException<T> consumer) {
    return lambdaSubscriber(consumer);
  }

  /**
   * Returns a processor that reacts to the <code>onNext</code> event. It is transparent to all
   * events.
   *
   * @param consumer the given function.
   * @param <T> the value type.
   * @return The processor.
   * @since 3.2.2
   */
  public static <T> Processor<T, T> onNextProcessor(final ConsumerWithException<T> consumer) {
    return probe(n -> {}, v -> tryToDoRethrow(() -> consumer.accept(v)));
  }

  static void parking(final Object blocker, final long timeout) {
    if (timeout != -1) {
      parkNanos(blocker, timeout * 1000);
    } else {
      park(blocker);
    }
  }

  /**
   * Pulls data from a processor without doing anything with it. You can use this to get a pipeline
   * started that ends with some side effect.
   *
   * @param processor the given processor.
   * @param <T> the incoming value type.
   * @param <R> theo outgoing value type.
   * @return The given processor as a subscriber.
   * @since 3.0
   */
  public static <T, R> Subscriber<T> pull(final Processor<T, R> processor) {
    processor.subscribe(devNull());

    return processor;
  }

  /**
   * Pulls data from a publisher without doing anything with it. You can use this to get a pipeline
   * started that ends with some side effect.
   *
   * @param publisher the given publisher.
   * @since 3.8.0
   */
  public static void pullForever(final Publisher<?> publisher) {
    publisher.subscribe(devNull());
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

    return box(result, buffer(1));
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

  /**
   * Creates a processor where a subscriber can tap the data that goes through it.
   *
   * @param subscriber the given subscriber.
   * @param <T> the value type.
   * @return The processor.
   * @since 3.0
   */
  public static <T> Processor<T, T> tap(final Subscriber<T> subscriber) {
    final Processor<T, T> pass1 = passThrough();
    final Processor<T, T> pass2 = passThrough();

    pass1.subscribe(Fanout.of(list(pass2, subscriber), list(true, false)));

    return combine(pass1, pass2);
  }

  static void trace(final Logger logger, final Supplier<String> message) {
    logger.finest(() -> logger.getName() + ": " + message.get());
  }

  public static <T, R> R transform(final Processor<T, R> processor, final T value) {
    return transformAsync(processor, value).toCompletableFuture().join();
  }

  public static <T, R> CompletionStage<R> transformAsync(
      final Processor<T, R> processor, final T value) {
    return asValueAsync(with(Source.of(value)).map(processor).get());
  }

  public static <T> Processor<T, T> throttle(final int maxPerSecond) {
    final Running running = new Running(maxPerSecond);

    return mapAsyncSequential(m -> running.update().thenApply(r -> m));
  }

  static void throwBackpressureViolation(
      final Object victim, final Subscription subscription, final long requested) {
    throw new GeneralException(
        "Backpressure violation in "
            + subscription.getClass().getName()
            + ". Requested "
            + requested
            + " elements in "
            + victim.getClass().getName()
            + ", which have already been received.");
  }

  private static class Retry<T> extends PassThrough<T> {
    private final Consumer<Throwable> onException;
    private final Supplier<Publisher<T>> publisher;
    private final Duration retryInterval;
    private long requested;

    private Retry(
        final Supplier<Publisher<T>> publisher,
        final Duration retryInterval,
        final Consumer<Throwable> onException) {
      this.publisher = publisher;
      this.retryInterval = retryInterval;
      this.onException = onException;
    }

    @Override
    protected void more(final long number) {
      dispatch(
          () -> {
            requested += number;
            super.more(number);
          });
    }

    @Override
    public void onComplete() {
      dispatch(super::onComplete);
    }

    @Override
    public void onError(Throwable t) {
      dispatch(
          () -> {
            setError(true);

            if (onException != null) {
              onException.accept(t);
            }

            if (!completed() && !cancelled()) {
              composeAsyncAfter(() -> completedFuture(publisher.get()), retryInterval)
                  .thenAccept(
                      p ->
                          dispatch(
                              () -> {
                                setError(false);
                                p.subscribe(this);
                              }));
            }
          });
    }

    @Override
    public void onNext(final T value) {
      dispatch(
          () -> {
            --requested;
            super.onNext(value);
          });
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
      if (this.subscription == null) {
        super.onSubscribe(subscription);
      } else {
        this.subscription = subscription; // Ignore rule 2.5 because that is the purpose here.

        if (requested > 0) {
          subscription.request(requested);
        }
      }
    }
  }

  private static class Running {
    private final int maxPerSecond;
    private long count = 0;
    private Instant second = now().truncatedTo(SECONDS);

    private Running(final int maxPerSecond) {
      this.maxPerSecond = maxPerSecond;
    }

    private CompletionStage<Boolean> update() {
      final Instant now = now().truncatedTo(SECONDS);

      if (between(second, now).toMillis() > 999) {
        count = 0;
        second = now;
      }

      final Duration remaining = between(now(), now.plusSeconds(1));

      return ++count >= maxPerSecond && !remaining.isNegative()
          ? supplyAsyncAfter(() -> true, remaining)
          : completedFuture(true);
    }
  }
}
