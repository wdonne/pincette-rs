package net.pincette.rs;

import static java.time.Duration.ofNanos;
import static java.util.Arrays.asList;
import static java.util.Arrays.fill;
import static java.util.logging.Logger.getLogger;
import static net.pincette.rs.Buffer.buffer;
import static net.pincette.rs.Util.throwBackpressureViolation;
import static net.pincette.rs.Util.trace;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Predicate;
import java.util.logging.Logger;

/**
 * A publisher that emits everything that the given publishers emit.
 *
 * @param <T> the value type.
 * @author Werner Donné
 * @since 3.0
 */
public class Merge<T> implements Publisher<T> {
  private final List<BranchSubscriber> branchSubscribers;
  private final Logger logger = getLogger(getClass().getName());
  private boolean completed;
  private final String key = UUID.randomUUID().toString();
  private long requestSequence;
  private Subscriber<? super T> subscriber;

  /**
   * Creates a merge publisher.
   *
   * @param publishers the publishers of which all events are forwarded.
   */
  public Merge(final List<? extends Publisher<T>> publishers) {
    branchSubscribers = publishers.stream().map(this::branchSubscriber).toList();
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

  private BranchSubscriber branchSubscriber(final Publisher<T> publisher) {
    final BranchSubscriber s = new BranchSubscriber();

    publisher.subscribe(s);

    return s;
  }

  private void dispatch(final Runnable action) {
    Serializer.dispatch(action::run, this::onError, key);
  }

  private void notifySubscriber() {
    if (subscriber != null && allSubscriptions()) {
      subscriber.onSubscribe(new Backpressure());
    }
  }

  private void onError(final Throwable t) {
    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  public void subscribe(final Subscriber<? super T> subscriber) {
    // The buffer makes sure all branches will be triggered. The buffer keeps it going because
    // one of the branches may stall and then the total requested amount would never be reached,
    // in which case the buffer wouldn't ask for more.
    final Processor<T, T> buffer = buffer(branchSubscribers.size(), ofNanos(0));

    this.subscriber = buffer;
    buffer.subscribe(subscriber);
    notifySubscriber();
  }

  private class Backpressure implements Subscription {
    private static long[] spreadRequests(final long n, final int numberSubscribers) {
      final long[] result = new long[numberSubscribers];

      fill(result, 0);

      long remaining = n;

      while (remaining > 0) {
        for (int i = 0; i < result.length && remaining > 0; ++i, --remaining) {
          result[i] += 1;
        }
      }

      return result;
    }

    public void cancel() {
      branchSubscribers.forEach(b -> b.subscription.cancel());
    }

    private List<BranchSubscriber> behind() {
      return selectSubscribers(s -> !s.complete && s.received < s.requested);
    }

    private List<BranchSubscriber> caughtUp() {
      return selectSubscribers(s -> !s.complete && s.received == s.requested);
    }

    public void request(final long n) {
      if (n <= 0) {
        throw new IllegalArgumentException("A request must be strictly positive.");
      }

      trace(logger, () -> "request: " + n);

      dispatch(
          () -> {
            if (!completed) {
              requestBranches(n);
            }
          });
    }

    private void requestBranch(final BranchSubscriber s, final long request) {
      dispatch(
          () -> {
            trace(logger, () -> "branch request: " + request + " for subscriber " + s);
            s.requested += request;
            s.subscription.request(request);
            s.sequence = ++requestSequence;
          });
    }

    private void requestBranches(final long request) {
      requestCandidates(
          Optional.of(caughtUp()).filter(l -> !l.isEmpty()).orElseGet(this::behind), request);
    }

    private void requestCandidates(final List<BranchSubscriber> candidates, final long request) {
      if (!candidates.isEmpty()) {
        final long[] requests = spreadRequests(request, candidates.size());

        for (int i = 0; i < requests.length; ++i) {
          if (requests[i] > 0) {
            requestBranch(candidates.get(i), requests[i]);
          }
        }
      }
    }

    private Comparator<BranchSubscriber> schedule() {
      return Comparator.<BranchSubscriber, Long>comparing(s -> s.requested - s.received)
          .thenComparing(s -> s.sequence);
    }

    private List<BranchSubscriber> selectSubscribers(final Predicate<BranchSubscriber> filter) {
      return branchSubscribers.stream().filter(filter).sorted(schedule()).toList();
    }
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
      trace(logger, () -> "onComplete for subscriber " + this);
      complete();
    }

    public void onError(final Throwable throwable) {
      subscriber.onError(throwable);
      cancelOthers();
    }

    public void onNext(final T item) {
      dispatch(
          () -> {
            trace(logger, () -> "Send onNext to subscriber " + this + ": " + item);

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
      } else {
        this.subscription = subscription;
        notifySubscriber();
      }
    }

    private void sendComplete() {
      dispatch(
          () -> {
            if (!completed) {
              completed = true;
              trace(logger, () -> "Send onComplete to subscriber " + this);
              subscriber.onComplete();
            }
          });
    }
  }
}
