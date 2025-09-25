package net.pincette.rs;

import static java.util.Arrays.asList;
import static java.util.logging.Logger.getLogger;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Buffer.buffer;
import static net.pincette.rs.Probe.probe;
import static net.pincette.rs.Util.roundRobinMergeRequest;
import static net.pincette.rs.Util.throwBackpressureViolation;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;
import net.pincette.util.Pair;

/**
 * A publisher that emits everything that the given publishers emit.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.0
 */
public class Merge<T> implements Publisher<T> {
  private static final Logger LOGGER = getLogger(Merge.class.getName());

  private Backpressure backpressure;
  private final List<BranchSubscriber> branchSubscribers;
  private boolean completed;
  private final Function<Long, Map<Integer, Long>> distributeRequests;
  private final String key = UUID.randomUUID().toString();
  private long requestSequence;
  private long requested;
  private Subscriber<? super T> subscriber;

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   */
  public Merge(final List<? extends Publisher<T>> publishers) {
    this(publishers, null);
  }

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   * @param distributeRequests the function that says how many requests should go to which publisher
   *     position. By default, the publishers that have caught up are selected and if there aren't
   *     any, the ones that are behind are selected.
   * @since 3.11.0
   */
  public Merge(
      final List<? extends Publisher<T>> publishers,
      final Function<Long, Map<Integer, Long>> distributeRequests) {
    branchSubscribers = publishers.stream().map(this::branchSubscriber).toList();
    this.distributeRequests = distributeRequests;
  }

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   * @param <T> the value type.
   * @return The new publisher.
   */
  public static <T> Publisher<T> of(final List<? extends Publisher<T>> publishers) {
    return new Merge<>(publishers);
  }

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   * @param <T> the value type.
   * @return The new publisher.
   */
  @SafeVarargs
  public static <T> Publisher<T> of(final Publisher<T>... publishers) {
    return new Merge<>(asList(publishers));
  }

  private boolean allSubscriptions() {
    return branchSubscribers.stream().allMatch(s -> s.subscription != null);
  }

  private List<Integer> behind() {
    return selectSubscribers(s -> !s.complete && s.received < s.requested);
  }

  private BranchSubscriber branchSubscriber(final Publisher<T> publisher) {
    final BranchSubscriber s = new BranchSubscriber();

    publisher.subscribe(s);

    return s;
  }

  private List<Integer> candidates() {
    return Optional.of(caughtUp()).filter(l -> !l.isEmpty()).orElseGet(this::behind);
  }

  private List<Integer> caughtUp() {
    return selectSubscribers(s -> !s.complete && s.received == s.requested);
  }

  private void dispatch(final Runnable action) {
    Serializer.dispatch(action::run, this::onError, key);
  }

  private void notifySubscriber() {
    if (subscriber != null && allSubscriptions()) {
      backpressure =
          new Backpressure(
              distributeRequests != null ? distributeRequests : this::requestCandidates);
      subscriber.onSubscribe(backpressure);
    }
  }

  private void onError(final Throwable t) {
    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  private Map<Integer, Long> requestCandidates(final long request) {
    return roundRobinMergeRequest(candidates(), request);
  }

  private void requestExtra() {
    trace(() -> "request extra: " + requested);
    backpressure.requestBranches(requested);
  }

  private Comparator<Pair<Integer, BranchSubscriber>> schedule() {
    return Comparator.<Pair<Integer, BranchSubscriber>, Long>comparing(
            pair -> pair.second.requested - pair.second.received)
        .thenComparing(pair -> pair.second.sequence);
  }

  private List<Integer> selectSubscribers(final Predicate<BranchSubscriber> filter) {
    return zip(rangeExclusive(0, branchSubscribers.size()), branchSubscribers.stream())
        .filter(pair -> filter.test(pair.second))
        .sorted(schedule())
        .map(pair -> pair.first)
        .toList();
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    // The buffer makes sure all branches will be triggered. The buffer keeps it going because
    // one of the branches may stall and then the total requested amount would never be reached,
    // in which case the buffer wouldn't ask for more.
    final Processor<T, T> buffer =
        box(
            probe(
                n -> trace(() -> "buffer requested: " + n),
                v ->
                    dispatch(
                        () -> {
                          trace(() -> "merge received: " + v);

                          if (--requested > 0) {
                            requestExtra();
                          }
                        })),
            buffer(branchSubscribers.size(), null, true));

    this.subscriber = buffer;
    buffer.subscribe(subscriber);
    notifySubscriber();
  }

  private class Backpressure implements Subscription {
    private final Function<Long, Map<Integer, Long>> distributeRequests;

    private Backpressure(final Function<Long, Map<Integer, Long>> distributeRequests) {
      this.distributeRequests = distributeRequests;
    }

    public void cancel() {
      branchSubscribers.forEach(b -> b.subscription.cancel());
    }

    public void request(final long n) {
      if (n <= 0) {
        throw new IllegalArgumentException("A request must be strictly positive.");
      }

      trace(() -> "dispatch request: " + n);

      dispatch(
          () -> {
            if (!completed) {
              trace(() -> "request branches: " + n);
              requested += n;
              requestBranches(n);
            }
          });
    }

    private void requestBranch(final BranchSubscriber s, final long request) {
      dispatch(
          () -> {
            trace(() -> "branch request: " + request + " for subscriber " + s);
            s.requested += request;
            s.subscription.request(request);
            s.sequence = ++requestSequence;
          });
    }

    private void requestBranches(final long request) {
      requestCandidates(distributeRequests.apply(request));
    }

    private void requestCandidates(final Map<Integer, Long> candidates) {
      candidates.entrySet().stream()
          .filter(e -> e.getValue() > 0)
          .forEach(e -> requestBranch(branchSubscribers.get(e.getKey()), e.getValue()));
    }
  }

  private void trace(final Supplier<String> message) {
    Util.trace(LOGGER, this, message);
  }

  private class BranchSubscriber implements Subscriber<T> {
    private boolean complete;
    private long received;
    private long requested;
    private long sequence;
    private Subscription subscription;

    private boolean allCompleted() {
      return branchSubscribers.stream().allMatch(s -> s.complete);
    }

    private void cancelOthers() {
      branchSubscribers.stream()
          .filter(s -> s != this)
          .map(s -> s.subscription)
          .forEach(Subscription::cancel);
    }

    private void complete() {
      dispatch(
          () -> {
            complete = true;

            if (allCompleted()) {
              sendComplete();
            }
          });
    }

    public void onComplete() {
      trace(() -> "onComplete for subscriber " + this);
      complete();
    }

    public void onError(final Throwable throwable) {
      subscriber.onError(throwable);
      cancelOthers();
    }

    public void onNext(final T item) {
      dispatch(
          () -> {
            trace(
                () ->
                    "Send onNext to subscriber "
                        + this
                        + ": "
                        + item
                        + ", received: "
                        + received
                        + ", requested: "
                        + requested);

            if (received == requested) {
              throwBackpressureViolation(this, subscription, requested);
            }

            ++received;
            subscriber.onNext(item);
          });
    }

    public void onSubscribe(final Subscription subscription) {
      if (subscription == null) {
        throw new NullPointerException("A subscription can't be null.");
      }

      if (this.subscription != null) {
        subscription.cancel();
      }

      this.subscription = subscription;
      notifySubscriber();
    }

    private void sendComplete() {
      dispatch(
          () -> {
            if (!completed) {
              completed = true;
              trace(() -> "Send onComplete to subscriber " + this);
              subscriber.onComplete();
            }
          });
    }
  }
}
