package net.pincette.rs;

import static java.lang.Math.abs;
import static java.util.logging.Logger.getLogger;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Probe.probe;
import static net.pincette.rs.Serializer.dispatch;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Splits processing over multiple shards using consistent hashing. This may be used in cases where
 * partial ordering is acceptable and more parallelism is desired.
 *
 * @param <T> the type of the incoming values.
 * @param <R> the type of the outgoing values.
 * @author Werner Donné
 * @since 3.11.0
 */
public class Sharded<T, R> implements Processor<T, R> {
  private static final Logger LOGGER = getLogger(Sharded.class.getName());

  private final int bufferSize;
  private boolean completed;
  private final Function<T, Integer> hashFunction;
  private final String key = UUID.randomUUID().toString();
  private final Publisher<R> merged;
  private final List<QueuePublisher<T>> shards;
  private Subscriber<? super R> subscriber;
  private boolean subscriberSetup;
  private Subscription subscription;

  /**
   * Creates a sharded processor. It uses the standard Java <code>hashCode</code> method and a
   * buffer size of 1000.
   *
   * @param processor the function that creates as many equal processors as there will be shards.
   *     Each processor receives a portion of the traffic.
   * @param numberOfShards the number of shards that will run in parallel.
   */
  public Sharded(final Supplier<Processor<T, R>> processor, final int numberOfShards) {
    this(processor, numberOfShards, Objects::hashCode, 1000);
  }

  /**
   * Creates a sharded processor.
   *
   * @param processor the function that creates as many equal processors as there will be shards.
   *     Each processor receives a portion of the traffic.
   * @param numberOfShards the number of shards that will run in parallel.
   * @param hashFunction the function that hashes the incoming elements. From the result the modulo
   *     of the number of shards is taken to determine which shard will get the element. The
   *     function must be consistent.
   * @param bufferSize the upstream request size, which is also the amount of elements that may be
   *     buffered in memory.
   */
  public Sharded(
      final Supplier<Processor<T, R>> processor,
      final int numberOfShards,
      final Function<T, Integer> hashFunction,
      final int bufferSize) {
    this.hashFunction = hashFunction;
    this.bufferSize = bufferSize;
    shards = queuePublishers(numberOfShards);
    merged = setUpShards(processor);
  }

  /**
   * Creates a sharded processor. It uses the standard Java <code>hashCode</code> method and a
   * buffer size of 1000.
   *
   * @param processor the function that creates as many equal processors as there will be shards.
   *     Each processor receives a portion of the traffic.
   * @param numberOfShards the number of shards that will run in parallel.
   */
  public static <T, R> Processor<T, R> sharded(
      final Supplier<Processor<T, R>> processor, final int numberOfShards) {
    return new Sharded<>(processor, numberOfShards);
  }

  /**
   * Creates a sharded processor.
   *
   * @param processor the function that creates as many equal processors as there will be shards.
   *     Each processor receives a portion of the traffic.
   * @param numberOfShards the number of shards that will run in parallel.
   * @param hashFunction the function that hashes the incoming elements. From the result the modulo
   *     of the number of shards is taken to determine which shard will get the element. The
   *     function must be consistent.
   * @param bufferSize the upstream request size, which is also the amount of elements that may be
   *     buffered in memory.
   */
  public static <T, R> Processor<T, R> sharded(
      final Supplier<Processor<T, R>> processor,
      final int numberOfShards,
      final Function<T, Integer> hashFunction,
      final int bufferSize) {
    return new Sharded<>(processor, numberOfShards, hashFunction, bufferSize);
  }

  private void more(final long request) {
    dispatch(
        () -> {
          if (!completed) {
            trace(() -> "request " + request);
            subscription.request(request);
          }
        },
        key);
  }

  @Override
  public void onComplete() {
    trace(() -> "dispatch onComplete");

    dispatch(
        () -> {
          trace(() -> "onComplete");
          completed = true;
          shards.forEach(QueuePublisher::close);
        },
        key);
  }

  @Override
  public void onError(final Throwable throwable) {
    shards.forEach(s -> s.onError(throwable));
  }

  @Override
  public void onNext(final T item) {
    trace(() -> "dispatch onNext");

    dispatch(
        () -> {
          trace(() -> "onNext: " + item);
          Optional.of(shards.get(abs(hashFunction.apply(item) % shards.size())))
              .filter(s -> !s.isClosed())
              .ifPresent(
                  s -> {
                    trace(() -> "Add to shard " + s);
                    s.getQueue().add(item);
                  });
        },
        key);
  }

  public void onSubscribe(final Subscription subscription) {
    if (this.subscription != null) {
      this.subscription.cancel();
    }

    this.subscription = subscription;
    setUpSubscriber();
  }

  private List<QueuePublisher<T>> queuePublishers(final int numberOfShards) {
    return rangeExclusive(0, numberOfShards).map(i -> new QueuePublisher<T>()).toList();
  }

  private Publisher<R> setUpShards(final Supplier<Processor<T, R>> processor) {
    return Merge.of(
        zip(rangeExclusive(0, shards.size()), shards.stream())
            .map(
                pair ->
                    with(pair.second)
                        .map(processor.get())
                        .map(
                            probe(
                                n ->
                                    trace(
                                        () ->
                                            "request for shard "
                                                + pair.first
                                                + ", with "
                                                + "size: "
                                                + shards.get(pair.first).getQueue().size()
                                                + ": "
                                                + n)))
                        .get())
            .toList());
  }

  private void setUpSubscriber() {
    if (!subscriberSetup && subscription != null && subscriber != null) {
      subscriberSetup = true;
      with(merged).map(probe(this::more)).buffer(bufferSize).get().subscribe(subscriber);
    }
  }

  @Override
  public void subscribe(Subscriber<? super R> subscriber) {
    this.subscriber = subscriber;
    setUpSubscriber();
  }

  private void trace(final Supplier<String> message) {
    Util.trace(LOGGER, this, message);
  }
}
